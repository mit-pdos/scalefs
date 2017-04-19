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

class memdisk : public disk
{
public:
  NEW_DELETE_OPS(memdisk);

  memdisk(u64 nbytes) : data_ptr_(nullptr), nbytes_(nbytes)
  {
    dk_nbytes = nbytes;
    snprintf(dk_busloc, sizeof(dk_busloc), "memide");
    snprintf(dk_model, sizeof(dk_model), "memide");

    u64 dptr_size = sizeof(char *) * (_fs_img_size/BSIZE);
    data_ptr_ = (u8**)kmalloc(dptr_size, "memide");
    assert(data_ptr_);
    memset(data_ptr_, 0, dptr_size);
  }

  void readv(kiovec *iov, int iov_cnt, u64 off) override
  {
    u8 *p;

    assert (iov_cnt == 1);
    u64 count = iov[0].iov_len;

    if(off > nbytes_ || off + count > nbytes_)
      panic("memdisk::readv: sector out of range: offset %ld, count %ld\n",
            off, count);

    assert(off % BSIZE == 0);
    p = data_ptr_[off / BSIZE];
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

    u64 blknum = off / BSIZE;
    for (int i = 0; i < iov_cnt; i++) {
      assert(iov[i].iov_len == BSIZE);
      p = data_ptr_[blknum];
      memmove(p, iov[i].iov_base, iov[i].iov_len);
      blknum++;
    }
  }

  void flush() override
  {
  }

public:
  u8** data_ptr_;
  const u64 nbytes_;
};

static memdisk* md;

// Allocate memory for the ramdisk intelligently:
// Our filesystem supports per-cpu inode- and block- allocators. So we use the
// same logic to identify the regions belonging to different CPUs among the disk
// blocks, and allocate (NUMA) node-local memory for those regions.
void
init_fs_state(const char *buf, u64 offset, u64 size)
{
  static superblock sb;
  static u32 current_blknum, read_upto_blknum;
  static u32 first_free_inode_block, first_free_bitmap_block;
  static u32 inodeblocks_per_cpu, bitmapblocks_per_cpu;

  assert(offset % BSIZE == 0 && size == BSIZE);
  current_blknum = offset/BSIZE;

  if (current_blknum % 100000 == 0)
    cprintf("Writing block %8d / %lu\r", current_blknum, _fs_img_size/BSIZE);

  if (!current_blknum) {
    assert(!md->data_ptr_[current_blknum]);
    md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", 0);
    goto out;
  }

  if (read_upto_blknum && current_blknum > read_upto_blknum) {
    // We should have allocated memory for this block already.
    assert(md->data_ptr_[current_blknum]);
    goto out;
  }

  if (current_blknum == 1) {
    // That's the superblock.
    assert(!md->data_ptr_[current_blknum]);
    md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", 0);
    memmove(&sb, buf, sizeof(sb));
    read_upto_blknum = BBLOCK(sb.size - 1, sb.ninodes);

    // Setup memory for all the per-cpu journals' datablocks.
    for (int cpuid = 0; cpuid < NCPU; cpuid++) {
      for (int blk = sb.journal_blknums[cpuid].start_blknum;
           blk <= sb.journal_blknums[cpuid].end_blknum; blk++) {

        // Some of these may not be datablocks (could be inode-blocks or
        // bitmap-blocks). If so, skip them, so that we can allocate memory
        // for them appropriately later on.
        if ((blk >= IBLOCK(1) && blk <= IBLOCK(sb.ninodes-1)) ||
           (blk >= BBLOCK(0, sb.ninodes) && blk <= BBLOCK(sb.size-1, sb.ninodes))) {
          continue;
        }

        assert(!md->data_ptr_[blk]);
        md->data_ptr_[blk] = (u8*) kmalloc(BSIZE, "memide-data", cpuid);
      }
    }

    goto out;
  }

  if (current_blknum >= IBLOCK(1) && current_blknum <= IBLOCK(sb.ninodes-1)) {
    // These are inode blocks.
    if (!first_free_inode_block) {
      const dinode *dip = (const dinode *) buf;
      if (!dip->type) {
        first_free_inode_block = current_blknum;
        inodeblocks_per_cpu = (sb.ninodes/IPB -
                              (first_free_inode_block - IBLOCK(1))) / NCPU;

        // Handle the case where there is just 1 inode block, and hence we cannot
        // split it per-cpu.
        if (!inodeblocks_per_cpu)
          inodeblocks_per_cpu = 1;
      } else {
        assert(!md->data_ptr_[current_blknum]);
        md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", 0);
        goto out;
      }
    }

    assert(first_free_inode_block && current_blknum >= first_free_inode_block);

    int cpuid = (current_blknum - first_free_inode_block) / inodeblocks_per_cpu;
    if (cpuid >= NCPU)
      cpuid = 0;

    assert(!md->data_ptr_[current_blknum]);
    md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", cpuid);
    goto out;
  }


  if (IBLOCK(sb.ninodes) != BBLOCK(0, sb.ninodes) &&
      current_blknum == IBLOCK(sb.ninodes)) {
    // Wasted (unused) block between the inode blocks and the bitmap blocks.
    assert(!md->data_ptr_[current_blknum]);
    md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", 0);
    goto out;
  }

  // The remaining blocks we look at are bitmap blocks.
  assert(current_blknum >= BBLOCK(0, sb.ninodes));

  if (!first_free_bitmap_block) {
    u8* p = (u8*) buf;
    if (p[0] == 0) {
      first_free_bitmap_block = current_blknum;
      bitmapblocks_per_cpu = (sb.size/BPB -
                             (first_free_bitmap_block - BBLOCK(0, sb.ninodes))) / NCPU;

      // Handle the case where there is just 1 bitmap block, and hence we cannot
      // split it per-cpu.
      if (!bitmapblocks_per_cpu)
        bitmapblocks_per_cpu = 1;
    } else {
      assert(!md->data_ptr_[current_blknum]);
      md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", 0);

      u32 start_bit = (current_blknum - BBLOCK(0, sb.ninodes)) * BPB;
      for (u32 i = start_bit; i < start_bit + BPB; i++) {
	// Be careful not to accidentally allocate memory for inode-blocks or
	// bitmap-blocks here. This loop should only deal with data-blocks.
	// (Note: the first bitmap block spans blocks starting all the way from
	// block 0 to BPB, which most likely covers all the inode-blocks and
	// bitmap-blocks; so we should take care to skip over them).
        if (i > BBLOCK(sb.size-1, sb.ninodes) && !md->data_ptr_[i])
          md->data_ptr_[i] = (u8*) kmalloc(BSIZE, "memide-data", 0);
      }

      goto out;
    }
  }

  assert(first_free_bitmap_block && current_blknum >= first_free_bitmap_block);

  {
    int cpuid = (current_blknum - first_free_bitmap_block) / bitmapblocks_per_cpu;
    if (cpuid >= NCPU)
      cpuid = 0;

    if (!md->data_ptr_[current_blknum])
      md->data_ptr_[current_blknum] = (u8*) kmalloc(BSIZE, "memide-data", cpuid);

    u32 start_bit = (current_blknum - BBLOCK(0, sb.ninodes)) * BPB;
    for (u32 i = start_bit; i < start_bit + BPB; i++) {
      if (i > BBLOCK(sb.size-1, sb.ninodes) && !md->data_ptr_[i])
        md->data_ptr_[i] = (u8*) kmalloc(BSIZE, "memide-data", cpuid);
    }
  }

out:
  memmove(md->data_ptr_[current_blknum], buf, BSIZE);
}

// Check that every logical disk block has a corresponding memory page backing it.
void
verify_backing_memory(u64 disk_size_bytes)
{
  u64 nblocks = disk_size_bytes/BSIZE;

  for (int i = 0; i < nblocks; i++)
    assert(md->data_ptr_[i]);
}

void
initmemdisk(void)
{
}

void
initdisk(void)
{
  md = new memdisk(_fs_img_size);
  disk_register(md);

  struct timeval before, after;

  cprintf("initdisk: Flashing the filesystem image on the memdisk(s)\n");

  gettimeofday(&before, NULL);
  zlib_decompress(_fs_imgz_start, _fs_imgz_size,
                  _fs_img_size, init_fs_state);

  verify_backing_memory(_fs_img_size);
  gettimeofday(&after, NULL);

  cprintf("Writing block %8lu / %lu\n", _fs_img_size/BSIZE, _fs_img_size/BSIZE);
  cprintf("Writing blocks ... done! (%d seconds)\n", after.tv_sec - before.tv_sec);
}

#endif  /* MEMIDE */
