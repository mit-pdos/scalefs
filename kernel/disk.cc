#include "types.h"
#include "kernel.hh"
#include "disk.hh"
#include "ideconfig.hh"
#include "vector.hh"
#include "amd64.h"
#include <cstring>
#include <sys/time.h>

#if AHCIIDE

#include "zlib-decompress.hh"
extern u8 _fs_imgz_start[];
extern u64 _fs_imgz_size;

static u64 nblocks = NMEGS * BLKS_PER_MEG;
static const u64 _fs_img_size = nblocks * BSIZE;

#define WB_SIZE	64*1024
static char write_buffer[WB_SIZE];
static u64 wb_offset;

void
write_output(const char *buf, u64 offset, u64 size)
{
   // TODO: Use disk_write in the asynchronous mode to make this faster. At the
   // moment, the scheduler panics ("EMBRYO -> 1") when the AHCI driver tries to
   // put the async request to sleep on the cmdslot_alloc_cv condvar inside
   // alloc_cmdslot(). This is probably because we are doing this way too early
   // in the boot sequence.

   assert(size == BSIZE);
   if ((offset/BSIZE) % 100000 == 0)
     cprintf("Writing block %8lu / %lu\r", offset/BSIZE, _fs_img_size/BSIZE);

   // We use disk_writev() to write out the data in bigger (accumulated) chunks
   // to the disk below, but its correctness heavily depends on the assumption
   // that all the writes are going to be sequential/contiguous (which holds
   // good for zlib_decompress(), the way it is implemented).
   memcpy(write_buffer + wb_offset, buf, size);
   wb_offset += size;

   // If the write-buffer is full or if this is the last write, write out all
   // the buffered data to the disk.
   if (wb_offset == WB_SIZE || offset == (_fs_img_size - BSIZE)) {
     kiovec iov = { (void *) write_buffer, wb_offset };
     disk_writev(1, &iov, 1, offset - (wb_offset - BSIZE));
     wb_offset = 0;
   }
}

void
initidedisk()
{
}

void
initdisk()
{
  struct timeval before, after;

  cprintf("initdisk: Flashing the filesystem image on the disk(s)\n");

  gettimeofday(&before, NULL);
  zlib_decompress(_fs_imgz_start, _fs_imgz_size,
                  _fs_img_size, write_output);

  gettimeofday(&after, NULL);

  cprintf("Writing block %8lu / %lu\n", _fs_img_size/BSIZE, _fs_img_size/BSIZE);
  cprintf("Writing blocks ... done! (%d seconds)\n", after.tv_sec - before.tv_sec);
}

#endif

static static_vector<disk*, NDISK> disks;

void
disk_register(disk* d)
{
  cprintf("disk_register: %s: %ld bytes: %s %s\n",
          d->dk_busloc, d->dk_nbytes, d->dk_model, d->dk_serial);
  disks.push_back(d);
}

static void
disk_test(disk *d)
{
  char buf[512];

  cprintf("testing disk %s\n", d->dk_busloc);

  cprintf("writing..\n");
  memset(buf, 0xab, 512);
  d->write(buf, 512, 0);

  cprintf("reading..\n");
  memset(buf, 0, 512);
  d->read(buf, 512, 0x2000);

  for (int i = 0; i < 512; i++)
    cprintf("%02x ", ((unsigned char*) buf)[i]);
  cprintf("\n");

  cprintf("flushing..\n");
  d->flush();

  cprintf("disk_test: test done\n");
}

static void
disk_test_all()
{
  for (disk* d : disks) {
    disk_test(d);
  }
}

//SYSCALL
void
sys_disktest(void)
{
  disk_test_all();
}

// Stripe across all the (four) disks, with a stripe size of 64KB.
#define STRIPE_SIZE_BLKS		(64 * 1024 / BSIZE)

// Given a block offset as argument, return the disk number that hosts that block.
u32 blknum_to_dev(u32 blknum)
{
  return (blknum / STRIPE_SIZE_BLKS) % num_disks();
}

u32 remap_blknum(u32 blknum)
{
  return STRIPE_SIZE_BLKS * ((blknum / STRIPE_SIZE_BLKS) / num_disks())
         + (blknum % STRIPE_SIZE_BLKS);
}

u32 num_disks()
{
  return (u32) disks.size();
}

void
disk_readv(u32 dev, kiovec *iov, int iov_cnt, u64 offset,
           sref<disk_completion> dc)
{
  assert(disks.size() > 0);
  assert(iov_cnt <= IOV_MAX);
  dev = blknum_to_dev(offset/BSIZE);
  offset = (u64)remap_blknum(offset/BSIZE) * BSIZE;

  if (dc) // Asynchronous
    disks[dev]->areadv(iov, iov_cnt, offset, dc);
  else
    disks[dev]->readv(iov, iov_cnt, offset);
}

void
disk_read(u32 dev, char* buf, u64 nbytes, u64 offset,
          sref<disk_completion> dc)
{
  kiovec iov = { (void*) buf, nbytes };
  disk_readv(dev, &iov, 1, offset, dc);
}

void
disk_writev(u32 dev, kiovec *iov, int iov_cnt, u64 offset,
            sref<disk_completion> dc)
{
  assert(disks.size() > 0);
  assert(iov_cnt <= IOV_MAX);
  dev = blknum_to_dev(offset/BSIZE);
  offset = (u64)remap_blknum(offset/BSIZE) * BSIZE;

  if (dc) // Asynchronous
    disks[dev]->awritev(iov, iov_cnt, offset, dc);
  else
    disks[dev]->writev(iov, iov_cnt, offset);
}

void
disk_write(u32 dev, const char* buf, u64 nbytes, u64 offset,
           sref<disk_completion> dc)
{
  kiovec iov = { (void*) buf, nbytes };
  disk_writev(dev, &iov, 1, offset, dc);
}

void
disk_flush(u32 dev, sref<disk_completion> dc)
{
  assert(dev < disks.size());
  if (dc) // Asynchronous
    disks[dev]->aflush(dc);
  else
    disks[dev]->flush();
}

