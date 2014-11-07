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

#include "buf.hh"

extern u8 _fs_imgz_start[];
extern u64 _fs_imgz_size;

#if MEMIDE

#include "zlib-decompress.hh"

static const u64 _fs_img_size = BSIZE*BLKCNT;
static unsigned char _fs_img_start[_fs_img_size];

static u8 *memdisk;

void
initdisk(void)
{
  zlib_decompress(_fs_imgz_start, _fs_imgz_size,
                  _fs_img_start, _fs_img_size);
  memdisk = (u8 *) _fs_img_start;
}

// Interrupt handler.
void
ideintr(void)
{
  // no-op
}

void
ideread(u32 dev, char* data, u64 count, u64 offset)
{
  u8 *p;

  if(dev != 1)
    panic("ideread: request not for disk 1");
  if(offset > _fs_img_size || offset + count > _fs_img_size)
    panic("ideread: sector out of range");

  p = memdisk + offset;
  memmove(data, p, count);
}

void
idewrite(u32 dev, const char* data, u64 count, u64 offset)
{
  u8 *p;

  if(dev != 1)
    panic("ideread: request not for disk 1");
  if(offset > _fs_img_size || offset + count > _fs_img_size)
    panic("ideread: sector out of range");

  p = memdisk + offset;
  memmove(p, data, count);
}

#endif  /* MEMIDE */
