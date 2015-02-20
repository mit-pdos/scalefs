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

extern u8 _fs_imgz_start[];
extern u64 _fs_imgz_size;

#if MEMIDE

#include "zlib-decompress.hh"

static const u64 _fs_img_size = BSIZE*BLKCNT;
static unsigned char _fs_img_start[_fs_img_size];

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
      panic("memdisk::readv: sector out of range");

    p = data_ + off;
    memmove(iov[0].iov_base, p, count);
  }

  void writev(kiovec *iov, int iov_cnt, u64 off) override
  {
    u8 *p;

    assert (iov_cnt == 1);
    u64 count = iov[0].iov_len;

    if(off > nbytes_ || off + count > nbytes_)
      panic("memdisk::writev: sector out of range");

    p = data_ + off;
    memmove(p, iov[0].iov_base, count);
  }

  void flush() override
  {
  }

private:
  u8* const data_;
  const u64 nbytes_;
};

void
initdisk(void)
{
  zlib_decompress(_fs_imgz_start, _fs_imgz_size,
                  _fs_img_start, _fs_img_size);

  memdisk* md = new memdisk(_fs_img_start, _fs_img_size);
  disk_register(md);
}

#endif  /* MEMIDE */
