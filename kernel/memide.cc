// Fake IDE disk; stores blocks in memory.
// Useful for running kernel without scratch disk.

#include "types.h"
#include "mmu.h"
#include "kernel.hh"
#include "spinlock.hh"
#include "condvar.hh"
#include "proc.hh"
#include "amd64.h"
#include "traps.h"
#include "disk.hh"
#include "buf.hh"
#include "ideconfig.hh"
#include <sys/time.h>

extern u8 _fs_imgz_start[];
extern u64 _fs_imgz_size;

#if MEMIDE

#include "zlib-decompress.hh"

static u64 nblocks = NMEGS * BLKS_PER_MEG;
static const u64 _fs_img_size = nblocks * BSIZE;
static unsigned char *_fs_img_start;

class memdisk : public disk
{
public:
  NEW_DELETE_OPS(memdisk);

  memdisk(u8 *data, u64 nbytes) : data_(data), nbytes_(nbytes)
  {
    dk_nbytes = nbytes;
    snprintf(dk_busloc, sizeof(dk_busloc), "memide");
    snprintf(dk_model, sizeof(dk_model), "memide");
  }

  void readv(kiovec *iov, int iov_cnt, u64 off) override
  {
    u8 *p;

    assert (iov_cnt == 1);
    u64 count = iov[0].iov_len;

    if(off > nbytes_ || off + count > nbytes_)
      panic("memdisk::readv: sector out of range: offset %ld, count %ld\n",
            off, count);

    p = data_ + off;
    memmove(iov[0].iov_base, p, count);
  }

  void writev(kiovec *iov, int iov_cnt, u64 off) override
  {
    u8 *p;

    // For simplicity, we mandate that all the I/O requests in the vector are of
    // equal size (BSIZE).

    u64 count = iov[0].iov_len;

    if (off > nbytes_ || off + iov_cnt * count > nbytes_)
      panic("memdisk::writev: sector out of range: offset %ld, count %ld\n",
             off, iov_cnt * count);

    p = data_ + off;
    for (int i = 0; i < iov_cnt; i++) {
      assert(iov[i].iov_len == BSIZE);
      memmove(p, iov[i].iov_base, iov[i].iov_len);
      p += BSIZE;
    }
  }

  void flush() override
  {
  }

private:
  u8* const data_;
  const u64 nbytes_;
};

void
write_output(const char *buf, u64 offset, u64 size)
{
  assert(size == BSIZE);
  if ((offset/BSIZE) % 100000 == 0)
    cprintf("Writing block %8lu / %lu\r", offset/BSIZE, _fs_img_size/BSIZE);
  memcpy(_fs_img_start + offset, buf, BSIZE);
}

void
initmemdisk(void)
{
  _fs_img_start = (unsigned char *)early_kalloc(_fs_img_size, PGSIZE);
}

void
initdisk(void)
{
  memdisk* md = new memdisk(_fs_img_start, _fs_img_size);
  disk_register(md);

  struct timeval before, after;

  cprintf("initdisk: Flashing the filesystem image on the memdisk(s)\n");

  gettimeofday(&before, NULL);
  zlib_decompress(_fs_imgz_start, _fs_imgz_size,
                  _fs_img_size, write_output);

  gettimeofday(&after, NULL);

  cprintf("Writing block %8lu / %lu\n", _fs_img_size/BSIZE, _fs_img_size/BSIZE);
  cprintf("Writing blocks ... done! (%d seconds)\n", after.tv_sec - before.tv_sec);
}

#endif  /* MEMIDE */
