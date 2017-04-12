#include "types.h"
#include "amd64.h"
#include "kernel.hh"
#include "pci.hh"
#include "pcireg.hh"
#include "disk.hh"
#include "ideconfig.hh"
#include "ahcireg.hh"
#include "satareg.hh"
#include "idereg.hh"
#include "kstream.hh"
#include "spinlock.hh"
#include "condvar.hh"
#include "cpu.hh"

enum { fis_debug = 0 };

static struct {
  char model[40];
  char serial[20];
} allowed_disks[] = {
  { "QEMU HARDDISK                          ", "QM00005            " },
  { "ST9500620NS                            ", "            9XF00VT" },
  { "HGST HTS721010A9E630                   ", "      JS10106200N9H" },
  { "HGST HTS721010A9E630                   ", "      JS10106200NDY" },
  { "HGST HTS721010A9E630                   ", "      JS10106200N5X" },
  { "HGST HTS721010A9E630                   ", "      JS10106200NGG" },
  { "Samsung SSD 850 PRO 256GB              ", "S39KNX0HC11959N    " },
  { "Samsung SSD 850 PRO 256GB              ", "S39KNX0HC11970B    " },
  { "Samsung SSD 850 PRO 256GB              ", "S39KNX0HC12319R    " },
  { "Samsung SSD 850 PRO 256GB              ", "S39KNX0HC12320V    " },
};

class ahci_hba;

struct ahci_port_mem
{
  volatile struct ahci_recv_fis rfis __attribute__((aligned (256)));
  u8 pad[0x300];

  volatile struct ahci_cmd_header cmdh[32] __attribute__((aligned (1024)));
  volatile struct ahci_cmd_table cmdt[32] __attribute__((aligned (128)));
};

class ahci_port : public disk
{
public:
  ahci_port(ahci_hba *h, int p, volatile ahci_reg_port* reg);

  void readv(kiovec *iov, int iov_cnt, u64 off) override;
  void writev(kiovec *iov, int iov_cnt, u64 off) override;
  void flush() override;

  void areadv(kiovec *iov, int iov_cnt, u64 off,
              sref<disk_completion> dc) override;
  void awritev(kiovec *iov, int iov_cnt, u64 off,
              sref<disk_completion> dc) override;
  void aflush(sref<disk_completion> dc) override;

  void handle_port_irq();
  void handle_error();
  void read_error_log();

  NEW_DELETE_OPS(ahci_port);

private:
  ahci_hba *const hba;
  const int pid;
  volatile ahci_reg_port *const preg;
  ahci_port_mem *portmem;
  int num_cmdslots;

  u64 fill_prd(int cmdslot, void* addr, u64 nbytes);
  u64 fill_prd_v(int, kiovec* iov, int iov_cnt);
  void fill_fis(int, sata_fis_reg* fis);

  void dump();
  int wait();

  void issue(int cmdslot, kiovec* iov, int iov_cnt, u64 off, int cmd,
             bool cmd_is_ncq = false);

  // For the disk read/write interface..
  u32 cmds_issued;
  int last_cmdslot;
  spinlock cmdslot_alloc_lock;
  condvar cmdslot_alloc_cv;
  sref<disk_completion> cmdslot_dc[32];

  int alloc_cmdslot(sref<disk_completion> dc, bool no_pending_ncq = false);

  void blocking_wait(sref<disk_completion> dc) {
    while (!dc->done()) {
      if (myproc()->get_state() == RUNNING) {
        dc->wait();
      } else {
        handle_port_irq();
      }
    }
  };
};

class ahci_hba : public irq_handler
{
public:
  ahci_hba(struct pci_func *pcif);
  ahci_hba(const ahci_hba &) = delete;
  ahci_hba &operator=(const ahci_hba &) = delete;

  static int attach(struct pci_func *pcif);

  void handle_irq() override;

  NEW_DELETE_OPS(ahci_hba);

private:
  const u32 membase;
  volatile ahci_reg *const reg;
  ahci_port* port[32];

public:
  const int ncs;  // max number of command slots in each port
};

void
initahci(void)
{
#if AHCIIDE
  pci_register_class_driver(PCI_CLASS_MASS_STORAGE,
                            PCI_SUBCLASS_MASS_STORAGE_SATA,
                            &ahci_hba::attach);
#endif
}

int
ahci_hba::attach(struct pci_func *pcif)
{
  if (PCI_INTERFACE(pcif->dev_class) != 0x01) {
    console.println("AHCI: not an AHCI controller");
    return 0;
  }

  console.println("AHCI: attaching");
  pci_func_enable(pcif);
  ahci_hba *hba __attribute__((unused)) = new ahci_hba(pcif);
  console.println("AHCI: done");
  return 1;
}

ahci_hba::ahci_hba(struct pci_func *pcif)
  : membase(pcif->reg_base[5]),
    reg((ahci_reg*) p2v(membase)),
    ncs(((reg->g.cap >> AHCI_CAP_NCS_SHIFT) & AHCI_CAP_NCS_MASK) + 1)
{
  reg->g.ghc |= AHCI_GHC_AE;

  for (int i = 0; i < 32; i++) {
    if (reg->g.pi & (1 << i)) {
      port[i] = new ahci_port(this, i, &reg->port[i].p);
    }
  }

  irq ahci_irq;

#ifdef HW_ben
  ahci_irq = pci_map_msi_irq(pcif);
#endif

  if (!ahci_irq.valid()) {
    // XXX Annoying that the device needs to know about the extpic.
    // Better if it just knew about PCI and PCI knew to do this.
    ahci_irq = extpic->map_pci_irq(pcif);
    // XXX Annoying that the device needs to know to only enable if it
    // came from the extpic.
    ahci_irq.enable();
  }

  ahci_irq.register_handler(this);
  reg->g.ghc |= AHCI_GHC_IE;
}

void
ahci_hba::handle_irq()
{
  for (int i = 0; i < 32; i++) {
    if (!(reg->g.is & (1 << i)))
      continue;

    if (port[i]) {
      port[i]->handle_port_irq();
    } else {
      cprintf("AHCI: stray irq for port %d, clearing\n", i);
    }

    /* AHCI 1.3, section 10.7.2.1 says we need to first clear the
     * port interrupt status and then clear the host interrupt
     * status.  It's fine to do this even after we've processed the
     * port interrupt: if any port interrupts happened in the mean
     * time, the host interrupt bit will just get set again. */
    reg->g.is = (1 << i);
  }
}


static void
ata_byteswap(char* buf, u64 len)
{
  for (u64 i = 0; i < len; i += 2) {
    char c = buf[i];
    buf[i] = buf[i+1];
    buf[i+1] = c;
  }
}


ahci_port::ahci_port(ahci_hba *h, int p, volatile ahci_reg_port* reg)
  : hba(h), pid(p), preg(reg), num_cmdslots(0), cmds_issued(0), last_cmdslot(-1)
{
  // Round up the size to make it an integral multiple of PGSIZE.
  // Crashes on boot otherwise.
  size_t portmem_size = (sizeof(ahci_port_mem) + PGSIZE-1) & ~(PGSIZE-1);
  portmem = (ahci_port_mem*) kmalloc(portmem_size, "ahci_port_mem");
  assert(portmem && portmem_size >= sizeof(ahci_port_mem));
  memset(portmem, 0, portmem_size);

  /* Wait for port to quiesce */
  if (preg->cmd & (AHCI_PORT_CMD_ST | AHCI_PORT_CMD_CR |
                   AHCI_PORT_CMD_FRE | AHCI_PORT_CMD_FR)) {
    cprintf("AHCI: port %d active, clearing..\n", pid);
    preg->cmd &= ~(AHCI_PORT_CMD_ST | AHCI_PORT_CMD_FRE);
    microdelay(500 * 1000);

    if (preg->cmd & (AHCI_PORT_CMD_CR | AHCI_PORT_CMD_FR)) {
      cprintf("AHCI: port %d still active, giving up\n", pid);
      return;
    }
  }

  /* Initialize memory buffers */
  for (int cmdslot = 0; cmdslot < 32; cmdslot++)
    portmem->cmdh[cmdslot].ctba = v2p((void*) &portmem->cmdt[cmdslot]);

  preg->clb = v2p((void*) &portmem->cmdh);
  preg->fb = v2p((void*) &portmem->rfis);
  preg->ci = 0;

  /* Clear any errors first, otherwise the chip wedges */
  preg->serr = ~0;
  preg->serr = 0;

  /* Enable receiving frames */
  preg->cmd |= AHCI_PORT_CMD_FRE | AHCI_PORT_CMD_ST |
               AHCI_PORT_CMD_SUD | AHCI_PORT_CMD_POD |
               AHCI_PORT_CMD_ACTIVE;

  /* Check if there's anything there */
  u32 phystat = preg->ssts;
  if (!phystat) {
    cprintf("AHCI: port %d: not connected\n", pid);
    return;
  }

  /* Try to send an IDENTIFY */
  union {
    struct identify_device id;
    char buf[512];
    u16 word[256];
  } id_buf;

  struct sata_fis_reg fis;
  memset(&fis, 0, sizeof(fis));
  fis.type = SATA_FIS_TYPE_REG_H2D;
  fis.cflag = SATA_FIS_REG_CFLAG;
  fis.command = IDE_CMD_IDENTIFY;
  fis.sector_count = 1;

  fill_prd(0, &id_buf, sizeof(id_buf));
  fill_fis(0, &fis);
  preg->ci = 1;

  if (wait() < 0) {
    cprintf("AHCI: port %d: cannot identify\n", pid);
    return;
  }

  if (!(id_buf.id.features86 & IDE_FEATURE86_LBA48)) {
    cprintf("AHCI: disk too small, driver requires LBA48\n");
    return;
  }

  u64 sectors = id_buf.id.lba48_sectors;
  dk_nbytes = sectors * 512;

  memcpy(dk_model, id_buf.id.model, sizeof(id_buf.id.model));
  ata_byteswap(dk_model, sizeof(dk_model));
  dk_model[sizeof(dk_model) - 1] = '\0';

  memcpy(dk_serial, id_buf.id.serial, sizeof(id_buf.id.serial));
  ata_byteswap(dk_serial, sizeof(dk_serial));
  dk_serial[sizeof(dk_serial) - 1] = '\0';

  memcpy(dk_firmware, id_buf.id.firmware, sizeof(id_buf.id.firmware));
  ata_byteswap(dk_firmware, sizeof(dk_firmware));
  dk_firmware[sizeof(dk_firmware) - 1] = '\0';

  snprintf(dk_busloc, sizeof(dk_busloc), "ahci.%d", pid);

  bool disk_allowed = false;
  for (int i = 0; i < sizeof(allowed_disks) / sizeof(allowed_disks[0]); i++) {
    if (!strcmp(dk_model,  allowed_disks[i].model) &&
        !strcmp(dk_serial, allowed_disks[i].serial))
      disk_allowed = true;
  }

  if (!disk_allowed) {
    cprintf("%s: disallowed AHCI disk: <%s> <%s>\n",
            dk_busloc, dk_model, dk_serial);
    return;
  }

  /* Check support for Native Command Queueing */
  if (!(id_buf.id.sata_caps & IDE_SATA_NCQ_SUPPORTED)) {
    cprintf("AHCI: port %d: SATA Native Command Queuing not supported\n", pid);
    return;
  }

  num_cmdslots = 1 + (id_buf.id.queue_depth & IDE_SATA_NCQ_QUEUE_DEPTH);
  if (num_cmdslots < hba->ncs)
    cprintf("AHCI: port %d: NCQ queue depth limited to %d (out of %d)\n",
            pid, num_cmdslots, hba->ncs);

  /* Enable write-caching, read look-ahead */
  memset(&fis, 0, sizeof(fis));
  fis.type = SATA_FIS_TYPE_REG_H2D;
  fis.cflag = SATA_FIS_REG_CFLAG;
  fis.command = IDE_CMD_SETFEATURES;
  fis.features = IDE_FEATURE_WCACHE_ENA;

  fill_prd(0, 0, 0);
  fill_fis(0, &fis);
  preg->ci = 1;

  if (wait() < 0) {
    cprintf("AHCI: port %d: cannot enable write caching\n", pid);
    return;
  }

  fis.features = IDE_FEATURE_RLA_ENA;
  fill_fis(0, &fis);
  preg->ci = 1;

  if (wait() < 0) {
    cprintf("AHCI: port %d: cannot enable read lookahead\n", pid);
    return;
  }

  // Switch off DMA Setup FIS Auto-Activate optimization for NCQ, as it
  // causes I/O errors sometimes.
#if 0
  // Enable DMA Setup FIS Auto-Activate optimization for NCQ
  fis.features = IDE_FEATURE_SATA_ENA;
  fis.sector_count = SATA_NCQ_AUTO_ACTIVATE;
  fill_fis(0, &fis);
  preg->ci = 1;

  if (wait() < 0) {
    cprintf("AHCI: port %d: cannot enable DMA setup auto-activate "
            "optimization for NCQ\n", pid);
    return;
  }
#endif

  if (VERBOSE) {
    // After enabling all the features, print out the updated data returned from
    // IDENTIFY DEVICE.
    memset(&fis, 0, sizeof(fis));
    fis.type = SATA_FIS_TYPE_REG_H2D;
    fis.cflag = SATA_FIS_REG_CFLAG;
    fis.command = IDE_CMD_IDENTIFY;
    fis.sector_count = 1;

    fill_prd(0, &id_buf, sizeof(id_buf));
    fill_fis(0, &fis);
    preg->ci = 1;

    if (wait() < 0) {
      cprintf("AHCI: port %d: cannot identify after enabling additional features\n",
              pid);
      return;
    }

    cprintf("\nIDENTIFY DEVICE data after enabling additional features on AHCI "
            "port %d\n", pid);

    for (int i = 0; i < 256; i++)
      cprintf("Word[%d]: 0x%x\n", i, id_buf.word[i]);
    cprintf("\n");
  }

  /* Enable interrupts */
#if 0
  preg->ie = AHCI_PORT_INTR_DEFAULT | AHCI_PORT_INTR_ERROR;
#else
  preg->ie = AHCI_PORT_INTR_DEFAULT;
#endif

  disk_register(this);
}

int
ahci_port::alloc_cmdslot(sref<disk_completion> dc, bool no_pending_ncq)
{
  scoped_acquire a(&cmdslot_alloc_lock);

  if (no_pending_ncq) {
    // Make sure that no NCQ commands are still in flight. This is primarily
    // used so that FLUSH CACHE (EXT) commands (which are not NCQ commands) are
    // not mixed with NCQ commands such as READ/WRITE FPDMA QUEUED. (The spec
    // mandates that non-NCQ commands must not be issued while any NCQ command
    // is still outstanding).

    while (preg->ci || preg->sact)
      cmdslot_alloc_cv.sleep(&cmdslot_alloc_lock);
  }

  for (;;) {

    bool all_scanned = false;
    for (int cmdslot = (last_cmdslot + 1) % num_cmdslots; cmdslot < num_cmdslots;
         cmdslot++) {

      if (!cmdslot_dc[cmdslot] && !(preg->ci & (1 << cmdslot)) &&
          !(preg->sact & (1 << cmdslot))) {

        cmdslot_dc[cmdslot] = dc;
        last_cmdslot = cmdslot;
        return cmdslot;
      }

      if (cmdslot == num_cmdslots - 1 && !all_scanned) {
        cmdslot = -1;
        all_scanned = true;
        continue;
      }
    }
    cmdslot_alloc_cv.sleep(&cmdslot_alloc_lock);
  }
}

u64
ahci_port::fill_prd_v(int cmdslot, kiovec* iov, int iov_cnt)
{
  u64 nbytes = 0;

  volatile ahci_cmd_table *cmd = (ahci_cmd_table *) &portmem->cmdt[cmdslot];
  assert(iov_cnt < sizeof(cmd->prdt) / sizeof(cmd->prdt[0]));

  for (int slot = 0; slot < iov_cnt; slot++) {
    cmd->prdt[slot].dba = v2p(iov[slot].iov_base);
    cmd->prdt[slot].dbc = iov[slot].iov_len - 1;
    cmd->prdt[slot].dbc |= 1 << 31;
    nbytes += iov[slot].iov_len;
  }

  portmem->cmdh[cmdslot].prdtl = iov_cnt;
  return nbytes;
}

u64
ahci_port::fill_prd(int cmdslot, void* addr, u64 nbytes)
{
  kiovec iov = { addr, nbytes };
  return fill_prd_v(cmdslot, &iov, 1);
}

static void
print_fis(sata_fis_reg *r)
{
  cprintf("SATA FIS Reg\n");
  cprintf("type:              0x%x\n", r->type);
  cprintf("cflag:             0x%x\n", r->cflag);
  cprintf("command/status:    0x%x\n", r->command);
  cprintf("features/error:    0x%x\n", r->features);
  cprintf("lba_0:             0x%x\n", r->lba_0);
  cprintf("lba_1:             0x%x\n", r->lba_1);
  cprintf("lba_2:             0x%x\n", r->lba_2);
  cprintf("dev_head:          0x%x\n", r->dev_head);
  cprintf("lba_3:             0x%x\n", r->lba_3);
  cprintf("lba_4:             0x%x\n", r->lba_4);
  cprintf("lba_5:             0x%x\n", r->lba_5);
  cprintf("features_ex:       0x%x\n", r->features_ex);
  cprintf("sector_count:      0x%x\n", r->sector_count);
  cprintf("sector_count_ex:   0x%x\n", r->sector_count_ex);
  cprintf("control:           0x%x\n", r->control);
}

void
ahci_port::fill_fis(int cmdslot, sata_fis_reg* fis)
{
  memcpy((void*) &portmem->cmdt[cmdslot].cfis[0], fis, sizeof(*fis));
  portmem->cmdh[cmdslot].flags = sizeof(*fis) / sizeof(u32);
  if (fis_debug)
    print_fis(fis);
}

void
ahci_port::dump()
{
  cprintf("AHCI port %d dump:\n", pid);
  cprintf("PxIS     = 0x%x\n", preg->is);
  cprintf("PxIE     = 0x%x\n", preg->ie);
  cprintf("PxCMD    = 0x%x\n", preg->cmd);
  cprintf("PxTFD    = 0x%x\n", preg->tfd);
  cprintf("PxSIG    = 0x%x\n", preg->sig);
  cprintf("PxCI     = 0x%x\n", preg->ci);
  cprintf("SStatus  = 0x%x\n", preg->ssts);
  cprintf("SControl = 0x%x\n", preg->sctl);
  cprintf("SError   = 0x%x\n", preg->serr);
  // cprintf("GHC      = 0x%x\n", hba->reg->ghc);
}

int
ahci_port::wait()
{
  u64 ts_start = rdtsc();

  for (;;) {
    u32 tfd = preg->tfd;
    u8 stat = AHCI_PORT_TFD_STAT(tfd);
    if (!(stat & IDE_STAT_BSY) && !(preg->ci & 1))
      return 0;

    u64 ts_diff = rdtsc() - ts_start;
    if (ts_diff > 1000 * 1000 * 1000) {
      cprintf("ahci_port::wait: stuck for %lx cycles\n", ts_diff);
      dump();
      return -1;
    }
  }
}

void
ahci_port::read_error_log()
{
  u8 buf[512];
  int iov_cnt = 1;
  kiovec iov = { (void*) buf, 512 };
  int offset = 0x10 * 512; // Offset in sectors must be 0x10.

  assert(!preg->ci && !preg->sact);

  auto dc = sref<disk_completion>::transfer(new disk_completion());
  int cmdslot = alloc_cmdslot(dc);

  // Read the first page (page 0) of the error log at address 10h.
  issue(cmdslot, &iov, iov_cnt, offset, IDE_CMD_READ_LOG_DMA_EXT);
  blocking_wait(dc);

  cprintf("NCQ Queued Error Log (10h):\n\n");
  for (int i = 0; i < 512; i++)
    cprintf("log_byte[%d]: 0x%x\n", i, buf[i]);
  cprintf("\n");
}

void
ahci_port::handle_error()
{
  cprintf("AHCI: port %d: ERROR! IS 0x%x, SERR 0x%x\n", pid, preg->is,
          preg->serr);

  u32 tfd = preg->tfd;
  if (AHCI_PORT_TFD_STAT(tfd) & (IDE_STAT_ERR | IDE_STAT_DF))
    cprintf("AHCI: port %d: status %02x, err %02x\n",
            pid, AHCI_PORT_TFD_STAT(tfd), AHCI_PORT_TFD_ERR(tfd));

  // Restart the port so that we can read the error log.
  preg->cmd &= ~AHCI_PORT_CMD_ST;
  if (preg->cmd & AHCI_PORT_CMD_CR) {
    microdelay(500 * 1000);

    if (preg->cmd & (AHCI_PORT_CMD_CR)) {
      cprintf("AHCI: port %d: unable to restart port upon error\n", pid);
      return;
    }
  }

  preg->serr = 0;
  preg->is = ~0;

  preg->cmd |= AHCI_PORT_CMD_ST;
  read_error_log();
  panic("AHCI port error\n");
}

void
ahci_port::handle_port_irq()
{
  scoped_acquire a(&cmdslot_alloc_lock);

#if 0
  if (preg->is & AHCI_PORT_INTR_ERROR)
    handle_error(); // Does not return!
#endif

  preg->is = ~0;

  for (int cmdslot = 0; cmdslot < 32; cmdslot++) {

    if (cmdslot_dc[cmdslot] && (cmds_issued & (1 << cmdslot)) &&
        !(preg->ci & (1 << cmdslot)) && !(preg->sact & (1 << cmdslot))) {

      cmdslot_dc[cmdslot]->notify();
      cmdslot_dc[cmdslot].reset();
      cmds_issued &= ~(1 << cmdslot);

      cmdslot_alloc_cv.wake_all();
    }
  }
}

void
ahci_port::readv(kiovec* iov, int iov_cnt, u64 off)
{
  auto dc = sref<disk_completion>::transfer(new disk_completion());
  areadv(iov, iov_cnt, off, dc);
  blocking_wait(dc);
}

void
ahci_port::areadv(kiovec* iov, int iov_cnt, u64 off,
                  sref<disk_completion> dc)
{
  int cmdslot = alloc_cmdslot(dc);
#if USE_SATA_NCQ
  issue(cmdslot, iov, iov_cnt, off, IDE_CMD_READ_FPDMA_QUEUED, true);
#else
  issue(cmdslot, iov, iov_cnt, off, IDE_CMD_READ_DMA_EXT);
#endif
}

void
ahci_port::writev(kiovec* iov, int iov_cnt, u64 off)
{
  auto dc = sref<disk_completion>::transfer(new disk_completion());
  awritev(iov, iov_cnt, off, dc);
  blocking_wait(dc);
}

void
ahci_port::awritev(kiovec* iov, int iov_cnt, u64 off,
                   sref<disk_completion> dc)
{
  int cmdslot = alloc_cmdslot(dc);
#if USE_SATA_NCQ
  issue(cmdslot, iov, iov_cnt, off, IDE_CMD_WRITE_FPDMA_QUEUED, true);
#else
  issue(cmdslot, iov, iov_cnt, off, IDE_CMD_WRITE_DMA_EXT);
#endif
}

void
ahci_port::flush()
{
  auto dc = sref<disk_completion>::transfer(new disk_completion());
  aflush(dc);
  blocking_wait(dc);
}

void
ahci_port::aflush(sref<disk_completion> dc)
{
#if USE_SATA_NCQ
  // FLUSH CACHE (EXT) is not an NCQ command and hence must not be issued if any
  // NCQ commands are still outstanding. So allocate a command slot only after
  // draining out all pending commands.
  int cmdslot = alloc_cmdslot(dc, true);
#else
  int cmdslot = alloc_cmdslot(dc);
#endif
  issue(cmdslot, nullptr, 0, 0, IDE_CMD_FLUSH_CACHE_EXT);
}

void
ahci_port::issue(int cmdslot, kiovec* iov, int iov_cnt, u64 off, int cmd,
                 bool cmd_is_ncq)
{
  assert((off % 512) == 0);

  sata_fis_reg fis;
  memset(&fis, 0, sizeof(fis));
  fis.type = SATA_FIS_TYPE_REG_H2D;
  fis.cflag = SATA_FIS_REG_CFLAG;
  fis.command = cmd;

  u64 len = fill_prd_v(cmdslot, iov, iov_cnt);
  assert((len % 512) == 0);
  assert(len <= (u64) MAX_PRD_SIZE * (u64) MAX_PRD_ENTRIES);

  portmem->cmdh[cmdslot].prdbc = 0;

  if (len) {
    fis.dev_head = IDE_DEV_LBA;
    fis.control = IDE_CTL_LBA48;

    u64 sector_off = off / 512;
    fis.lba_0 = (sector_off >>  0) & 0xff;
    fis.lba_1 = (sector_off >>  8) & 0xff;
    fis.lba_2 = (sector_off >> 16) & 0xff;
    fis.lba_3 = (sector_off >> 24) & 0xff;
    fis.lba_4 = (sector_off >> 32) & 0xff;
    fis.lba_5 = (sector_off >> 40) & 0xff;

    u64 num_sectors = len / 512;
    if (cmd_is_ncq) {
      fis.features = num_sectors & 0xff;
      fis.features_ex = (num_sectors >> 8) & 0xff;
      fis.sector_count = cmdslot << 3;
    } else {
      fis.sector_count = num_sectors & 0xff;
      fis.sector_count_ex = (num_sectors >> 8) & 0xff;
    }
  }

  fill_fis(cmdslot, &fis);

  // Update the Write bit in the flags *after* invoking fill_fis(), to ensure
  // that it remains set (and hence allow the disk write to go through).
  // Otherwise, disk writes never complete on ben.
  if (cmd == IDE_CMD_WRITE_DMA_EXT || cmd == IDE_CMD_WRITE_FPDMA_QUEUED)
    portmem->cmdh[cmdslot].flags |= AHCI_CMD_FLAGS_WRITE;

  // Mark the command as issued, for the interrupt handler's benefit.
  // The cmdslot_alloc_lock protects 'cmds_issued' as well.
  scoped_acquire a(&cmdslot_alloc_lock);
  cmds_issued |= (1 << cmdslot);

  if (cmd_is_ncq)
    preg->sact = (1 << cmdslot);

  preg->ci = (1 << cmdslot);
}
