#include "types.h"
#include "kernel.hh"
#include "disk.hh"
#include "vector.hh"
#include "amd64.h"
#include <cstring>

#if AHCIIDE

#include "zlib-decompress.hh"
extern u8 _fs_imgz_start[];
extern u64 _fs_imgz_size;

static u64 nblocks = NMEGS * BLKS_PER_MEG;
static const u64 _fs_img_size = nblocks * BSIZE;
static unsigned char *_fs_img_start;


void initidedisk()
{
   _fs_img_start = (unsigned char *)early_kalloc(_fs_img_size, PGSIZE);
}

void initdisk()
{
   zlib_decompress(_fs_imgz_start, _fs_imgz_size,
                   _fs_img_start, _fs_img_size);

   cprintf("initdisk: Flashing the filesystem image on the disk(s)\n");

   // TODO: Use idewrite_async() to make this faster. At the moment, the
   // scheduler panics ("EMBRYO -> 1") when the AHCI driver tries to put
   // the async request to sleep on the cmdslot_alloc_cv condvar inside
   // alloc_cmdslot(). This is probably because we are doing this way too
   // early in the boot sequence.

   for (u64 i = 0; i < _fs_img_size; i += BSIZE)
     idewrite(1, (char *)_fs_img_start + i, BSIZE, i);
}

#endif

static static_vector<disk*, 64> disks;

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

// compat for a single IDE disk..
void
ideread(u32 dev, char* data, u64 count, u64 offset)
{
  assert(disks.size() > 0);
  disks[0]->read(data, count, offset);
}

void
ideread_async(u32 dev, char* data, u64 count, u64 offset,
              sref<disk_completion> dc)
{
  assert(disks.size() > 0);
  disks[0]->aread(data, count, offset, dc);
}

void
idewrite(u32 dev, const char* data, u64 count, u64 offset)
{
  assert(disks.size() > 0);
  disks[0]->write(data, count, offset);
}

void
idewrite_async(u32 dev, const char* data, u64 count, u64 offset,
               sref<disk_completion> dc)
{
  assert(disks.size() > 0);
  disks[0]->awrite(data, count, offset, dc);
}

void
ideflush()
{
  assert(disks.size() > 0);
  disks[0]->flush();
}

