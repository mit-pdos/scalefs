// File system implementation.  Four layers:
//   + Blocks: allocator for raw disk blocks.
//   + Files: inode allocator, reading, writing, metadata.
//   + Directories: inode with special contents (list of other inodes!)
//   + Names: paths like /usr/rtm/xv6/fs.c for convenient naming.
//
// Disk layout is: superblock, inodes, block in-use bitmap, data blocks.
//
// This file contains the low-level file system manipulation
// routines.  The (higher-level) system call implementations
// are in sysfile.c.

/*
 * inode cache will be RCU-managed:
 *
 * - to evict, mark inode as a victim
 * - lookups that encounter a victim inode must return an error (-E_RETRY)
 * - E_RETRY rolls back to the beginning of syscall/pagefault and retries
 * - out-of-memory error should be treated like -E_RETRY
 * - once an inode is marked as victim, it can be gc_delayed()
 * - the do_gc() method should remove inode from the namespace & free it
 *
 * - inodes have a refcount that lasts beyond a GC epoch
 * - to bump refcount, first bump, then check victim flag
 * - if victim flag is set, reduce the refcount and -E_RETRY
 *
 */

#include "types.h"
#include <uk/stat.h>
#include "mmu.h"
#include "kernel.hh"
#include "spinlock.hh"
#include "condvar.hh"
#include "proc.hh"
#include "fs.h"
#include "file.hh"
#include "cpu.hh"
#include "kmtrace.hh"
#include "dirns.hh"
#include "kstream.hh"
#include "scalefs.hh"

#define BLOCKROUNDUP(off) (((off)%BSIZE) ? (off)/BSIZE+1 : (off)/BSIZE)

// A hash-table to cache in-memory inode data-structures.
static chainhash<pair<u32, u32>, inode*> *ins;

static struct freeinum_bitmap freeinum_bitmap;

static sref<inode> the_root;
static struct superblock sb_root;

// Read the super block.
static void
readsb(int dev, struct superblock *sb)
{
  sref<buf> bp = buf::get(dev, 1);
  auto copy = bp->read();
  memmove(sb, copy->data, sizeof(*sb));
}

void
get_superblock(struct superblock *sb)
{
  sb->size = sb_root.size;
  sb->ninodes = sb_root.ninodes;
  sb->nblocks = sb_root.nblocks;
}

// Zero the in-memory buffer-cache block corresponding to a disk block.
// If @writeback == true, immediately write back the zeroed block to disk
// (this is useful when clearing the journal's disk blocks).
static void
bzero(int dev, int bno, bool writeback = false)
{
  sref<buf> bp = buf::get(dev, bno, true);
  {
    auto locked = bp->write();
    memset(locked->data, 0, BSIZE);
  }
  if (writeback)
    bp->writeback_async();
}

class out_of_blocks : public std::exception
{
  virtual const char* what() const throw() override
  {
    return "Out of blocks";
  }
};

static void inline
throw_out_of_blocks()
{
#if EXCEPTIONS
  throw out_of_blocks();
#else
  panic("out of blocks");
#endif
}

// Allocate a disk block. This makes changes only to the in-memory
// free-bit-vector (maintained by rootfs_interface), not the one on the disk.
static u32
balloc(u32 dev, transaction *trans = NULL, bool zero_on_alloc = false)
{
  int b;

  if (dev == 1) {
    b = rootfs_interface->alloc_block();
    if (b < sb_root.size) {
      if (trans)
        trans->add_allocated_block(b);

      if (zero_on_alloc)
        bzero(dev, b);
      return b;
    }
  }

  throw_out_of_blocks();
  // Unreachable
  return 0;
}

// Free a disk block. We never zero out blocks during free (we do that only
// during allocation, if desired).
//
// This makes changes only to the in-memory free-bit-vector (maintained by
// rootfs_interface), not the one on the disk.
//
// delayed_free = true indicates that the block should not be marked free in the
// in-memory free-bit-vector just yet. This is delayed until the time that the
// transaction is processed. We need this to ensure that the blocks freed in a
// transaction are not available for reuse until that transaction commits.
static void
bfree(int dev, u64 x, transaction *trans = NULL, bool delayed_free = false)
{
  u32 b = x;

  if (dev == 1) {
    if (!delayed_free)
      rootfs_interface->free_block(b);
    if (trans)
      trans->add_free_block(b);
    return;
  }
}

// Mark blocks as allocated or freed in the on-disk bitmap.
// Allocate if @alloc == true, free otherwise.
// The caller must provide a sorted block list.
void
balloc_free_on_disk(std::vector<u32>& blocks, transaction *trans, bool alloc)
{
  // Aggregate all updates to the same free bitmap block and write it out
  // just once, using a single transaction_diskblock.
  for (auto bno = blocks.begin(); bno != blocks.end(); ) {
    u32 blocknum = BBLOCK(*bno, sb_root.ninodes);
    sref<buf> bp = buf::get(1, blocknum);
    auto locked = bp->write();

    // Record the highest block-number represented in this free bitmap block,
    // to facilitate merging of all updates that touch the same bitmap block.
    u32 max_bno = *bno | (BPB - 1);

    do {
      int bi = *bno % BPB;
      int m = 1 << (bi % 8);
      if (alloc) {
        if ((locked->data[bi/8] & m) != 0)
          panic("balloc_free_on_disk: block %d already in use", *bno);
        locked->data[bi/8] |= m;
      } else {
        if ((locked->data[bi/8] & m) == 0)
          panic("balloc_free_on_disk: block %d already free", *bno);
        locked->data[bi/8] &= ~m;
      }
    } while (++bno && bno != blocks.end() && *bno <= max_bno);

    bp->add_to_transaction(trans);
  }
}


// Inodes.
//
// An inode is a single, unnamed file in the file system. The inode disk
// structure holds metadata (the type, device numbers, and data size) along
// with a list of blocks where the associated data can be found.
//
// The inodes are laid out sequentially on disk immediately after the
// superblock.  The kernel keeps a cache of the in-use on-disk structures
// to provide a place for synchronizing access to inodes shared between
// multiple processes.
//
// ip->ref counts the number of pointer references to this cached inode;
// references are typically kept in struct file and in proc->cwd. When ip->ref
// falls to zero, the inode is no longer cached. It is an error to use an
// inode without holding a reference to it.
//
// Processes are only allowed to read and write inode metadata and contents
// when holding the inode's lock, represented by the 'readbusy' and 'busy'
// flags in the in-memory copy. Because inode locks are held during disk
// accesses, they are implemented using a flag rather than with spin locks.
// Callers are responsible for locking inodes before passing them to routines
// in this file; leaving this responsibility with the caller makes it possible
// for them to create arbitrarily-sized atomic operations.
//
// To give maximum control over locking to the callers, the routines in this
// file that return inode pointers return pointers to *unlocked* inodes (except
// ialloc() which returns a locked inode to prevent races on freshly created
// inodes). It is the callers' responsibility to lock them before using them.
// A non-zero ip->ref keeps these unlocked inodes in the cache.


// Initialize the freeinum_bitmap from the disk when the system boots.
static void
initialize_freeinum_bitmap(void)
{
  sref<buf> bp;
  superblock sb;
  int ninums;
  u32 first_free_inodeblock_inum = 0;

  get_superblock(&sb);

  // Allocate the memory for the inum_vector in one shot, instead of doing it
  // piecemeal using .push_back() in a loop.
  freeinum_bitmap.inum_vector.reserve(sb.ninodes);

  for (u32 inum = 0; inum < sb.ninodes; inum += IPB) {
    bp = buf::get(1, IBLOCK(inum));
    auto copy = bp->read();

    ninums = std::min((u32)IPB, sb.ninodes - inum);

    for (int i = 0; i < ninums; i++) {
      const dinode *dip = (const struct dinode*)copy->data + (inum + i)%IPB;

      // Maintain a vector as well as a linked-list representation of the free
      // inums, to speed up freeing and allocation of blocks, respectively.
      free_inum *finum = new free_inum(inum + i, !dip->type);
      freeinum_bitmap.inum_vector.push_back(finum);

      // Make note of the first inode block (inum) that starts with a free inum
      // (which is an approximation that, that entire inode block (and all
      // the subsequent ones) contains only free inums). That's where we'll
      // start allocating per-CPU resources from (further down in the code),
      // in order to avoid initializing CPU0 with nearly no free inums.
      if (!first_free_inodeblock_inum && !dip->type && i == 0)
        first_free_inodeblock_inum = inum;
    }
  }

  // Distribute the inums among the CPUs and add the free inums to the per-CPU
  // freelists.

  // TODO: Remove this assert and handle cases where multiple CPUs have to share
  // the same inode blocks.
  static_assert(NINODES/IPB >= NCPU, "No. of inode blocks < NCPU\n");

  u32 ninodeblocks = sb.ninodes/IPB - first_free_inodeblock_inum/IPB;
  u32 inodeblocks_per_cpu = ninodeblocks/NCPU;
  u32 inums_per_cpu = inodeblocks_per_cpu * IPB;

  for (int cpu = 0; cpu < NCPU; cpu++) {
    auto list_lock = freeinum_bitmap.freelists[cpu].list_lock.guard();

    if (VERBOSE)
      cprintf("Per-CPU inode allocator: CPU %d   inodes [%u - %u]\n",
              cpu, first_free_inodeblock_inum + cpu * inums_per_cpu,
              first_free_inodeblock_inum + ((cpu+1) * inums_per_cpu) - 1);

    for (u32 inum = cpu * inums_per_cpu; inum < (cpu+1) * inums_per_cpu; inum++) {
      if (!inum)
        continue; // inum 0 is not used, so don't add it to any freelist.

      auto finum = freeinum_bitmap.inum_vector.at(inum +
                                                  first_free_inodeblock_inum);
      finum->cpu = cpu;
      if (finum->is_free)
        freeinum_bitmap.freelists[cpu].inum_freelist.push_back(finum);
    }
  }

  // Build a global reserve pool of free inums using whatever is remaining,
  // to be used when a per-CPU freelist runs out, before stealing free inums
  // from other CPUs' freelists.
  if (NCPU * inodeblocks_per_cpu < ninodeblocks) {
    auto list_lock = freeinum_bitmap.reserve_freelist.list_lock.guard();
    for (u32 inum = NCPU * inums_per_cpu;
         inum + first_free_inodeblock_inum < sb.ninodes; inum++) {
      if (!inum)
        continue; // inum 0 is not used, so don't add it to any freelist.

      auto finum = freeinum_bitmap.inum_vector.at(inum +
                                                  first_free_inodeblock_inum);
      finum->cpu = NCPU; // Invalid CPU number to denote reserve pool.
      if (finum->is_free)
        freeinum_bitmap.reserve_freelist.inum_freelist.push_back(finum);
    }
  }

  // Any other leftover free inums from [1 to first_free_inodeblock_inum) also
  // go to the reserve pool. Also make sure to set the CPU number for those
  // free-inums irrespective of whether they are free.
  {
    auto list_lock = freeinum_bitmap.reserve_freelist.list_lock.guard();
    for (u32 inum = 1; inum < first_free_inodeblock_inum; inum++) {
      auto finum = freeinum_bitmap.inum_vector.at(inum);
      finum->cpu = NCPU; // Invalid CPU number to denote reserve pool.
      if (finum->is_free)
        freeinum_bitmap.reserve_freelist.inum_freelist.push_back(finum);
    }
  }
}

// Allocate an inode number from the freeinum_bitmap.
static u32
alloc_inode_number(void)
{
  u32 inum;
  int cpu = myid();
  static bool warned_once = false;

  // Use the linked-list representation of the free-inums to perform inum
  // allocation in O(1) time. This list only contains the inums that are
  // actually free, so we can allocate any one of them.

  {
    auto list_lock = freeinum_bitmap.freelists[cpu].list_lock.guard();

    if (!freeinum_bitmap.freelists[cpu].inum_freelist.empty()) {
      auto it = freeinum_bitmap.freelists[cpu].inum_freelist.begin();
      assert(it->is_free);
      it->is_free = false;
      inum = it->inum_;
      freeinum_bitmap.freelists[cpu].inum_freelist.erase(it);
      return inum;
    }
  }

  // If we run out of inums in our local CPU's freelist, tap into the global
  // reserve pool first.
  if (VERBOSE && !warned_once) {
    cprintf("WARNING: alloc_inum(): CPU %d allocating inums from the global "
            "reserve pool.\nThis could be a sign that inums are getting "
            "leaked!\n", cpu);
    warned_once = true;
  }

  // TODO: Allocate from the reserve pool in bulk in order to reduce the
  // chances of contention even further.
  {
    if (freeinum_bitmap.reserve_freelist.inum_freelist.empty())
      goto try_neighbor;

    auto list_lock = freeinum_bitmap.reserve_freelist.list_lock.guard();

    if (!freeinum_bitmap.reserve_freelist.inum_freelist.empty()) {
      auto it = freeinum_bitmap.reserve_freelist.inum_freelist.begin();
      assert(it->is_free);
      it->is_free = false;
      inum = it->inum_;
      freeinum_bitmap.reserve_freelist.inum_freelist.erase(it);
      return inum;
    }
  }

  // We failed to allocate even from the reserve pool. So steal free inums
  // from other CPUs. Each CPU starts its fallback-search at a different
  // point, in order to avoid hotspots. Note that these inums are only
  // borrowed temporarily and are prompty returned to the original CPU's
  // freelists upon being freed.
try_neighbor:
  for (int fallback_cpu = cpu + 1; fallback_cpu % NCPU != cpu; fallback_cpu++) {
    int fcpu = fallback_cpu % NCPU;

    if (freeinum_bitmap.freelists[fcpu].inum_freelist.empty())
      continue;

    auto list_lock = freeinum_bitmap.freelists[fcpu].list_lock.guard();

    if (!freeinum_bitmap.freelists[fcpu].inum_freelist.empty()) {
      auto it = freeinum_bitmap.freelists[fcpu].inum_freelist.begin();
      assert(it->is_free);
      it->is_free = false;
      inum = it->inum_;
      freeinum_bitmap.freelists[fcpu].inum_freelist.erase(it);
      return inum;
    }
  }

  panic("alloc_inum(): Out of inums on CPU %d\n", cpu);
  return 0; // out of inode numbers
}

// Mark an inode number as free in the freeinum_bitmap.
void
free_inode_number(u32 inum)
{
  // Use the vector representation of the free-inums to free the inum in
  // O(1) time (by optimizing the blocknumber-to-free_inum lookup).
  free_inum *finum = freeinum_bitmap.inum_vector.at(inum);

  int cpu = finum->cpu;
  if (cpu < NCPU) {
    auto list_lock = freeinum_bitmap.freelists[cpu].list_lock.guard();
    assert(!finum->is_free);
    finum->is_free = true;
    freeinum_bitmap.freelists[cpu].inum_freelist.push_front(finum);
  } else {
    // This inum belongs to the global reserve pool.
    auto list_lock = freeinum_bitmap.reserve_freelist.list_lock.guard();
    assert(!finum->is_free);
    finum->is_free = true;
    freeinum_bitmap.reserve_freelist.inum_freelist.push_front(finum);
  }
}

// This is invoked before performing filesystem crash-recovery.
void
initinode_early(void)
{
  scoped_gc_epoch e;

  readsb(ROOTDEV, &sb_root); // Initialize sb_root by reading the superblock.
  ins = new chainhash<pair<u32, u32>, inode*>(NINODES_PRIME);

  the_root = inode::alloc(ROOTDEV, ROOTINO);
  if (!ins->insert(make_pair(the_root->dev, the_root->inum), the_root.get()))
    panic("initinode_early: Failed to insert the root inode into the cache\n");
  the_root->init();

}

// This is invoked after performing filesystem crash-recovery.
void
initinode_late(void)
{
  // Re-initialize the root directory after crash-recovery, so as to reflect the
  // most up-to-date state.
  the_root->init();
  initialize_freeinum_bitmap();
}

// Returns an inode locked for write, on success.
static sref<inode>
try_ialloc(u32 inum, u32 dev, short type)
{
  sref<inode> ip = iget(dev, inum);
  if (ip->type || !cmpxch(&ip->type, (short) 0, type))
    return sref<inode>();

  ilock(ip, WRITELOCK);
  ip->gen += 1;
  if (ip->nlink() || ip->size || ip->addrs[0])
    panic("try_ialloc: inode not zeroed\n");
  return ip;
}

// Allocate a new inode with the given type on device dev.
// Returns an inode locked for write, on success.
sref<inode>
ialloc(u32 dev, short type)
{
  scoped_gc_epoch e;
  sref<inode> ip;

  u32 inum = alloc_inode_number();
  if (inum) {
    ip = try_ialloc(inum, dev, type);
    if (ip)
      return ip;
  }

  cprintf("ialloc: 0/%u inodes\n", sb_root.ninodes);
  return sref<inode>();
}

void
free_inode(sref<inode> ip, transaction *tr)
{
  ilock(ip, WRITELOCK);
  assert(ip->nlink() == 0);
  // Postpone reusing this inode number until transaction commit.
  tr->add_free_inum(ip->inum);
  // Release the inode on the disk.
  ip->type = 0;
  iupdate(ip, tr);

  // Perform the last decrement of the refcount. This pairs with the
  // extra increment that was done inside inode::init().
  ip->dec();
  iunlock(ip);
}

// Propagate the changes made to the in-memory inode metadata, to the disk.
// As far as possible, don't invoke iupdate() on every little change to the
// inode; batch the updates and call iupdate() once at the end, to avoid the
// scalability bottleneck (and overhead) of repeated copies to the buffer-cache
// under the buf's write-lock.
//
// The caller must hold ilock at least for read (but the caller will typically
// need to hold it for write, in order to log the correct snapshot of the inode
// to the transaction).
void
iupdate(sref<inode> ip, transaction *trans)
{
  scoped_gc_epoch e;

  sref<buf> bp = buf::get(ip->dev, IBLOCK(ip->inum));
  auto locked = bp->write();

  dinode *dip = (struct dinode*)locked->data + ip->inum%IPB;
  dip->type = ip->type;
  dip->major = ip->major;
  dip->minor = ip->minor;
  dip->nlink = ip->nlink();
  dip->size = ip->size;
  dip->gen = ip->gen;
  memmove(dip->addrs, ip->addrs, sizeof(ip->addrs));
  bp->add_to_transaction(trans);
}

inode::inode(u32 d, u32 i)
  : rcu_freed("inode", this, sizeof(*this)), dev(d), inum(i),
    valid(false), busy(false), readbusy(0), dir(nullptr), dir_offset(0)
{
}

inode::~inode()
{
  if (dir) {
    dir->remove(strbuf<DIRSIZ>("."));
    dir->remove(strbuf<DIRSIZ>(".."));
    delete dir;
  }
}

sref<inode>
iget(u32 dev, u32 inum)
{
  // Assumes caller is holding a gc_epoch

 retry:
  // Try for cached inode.
  inode *iptr = nullptr;
  sref<inode> ip;

  if (ins->lookup(make_pair(dev, inum), &iptr)) {
    ip = sref<inode>::newref(iptr);

    if (!ip->valid.load()) {
      acquire(&ip->lock);
      while (!ip->valid)
        ip->cv.sleep(&ip->lock);
      release(&ip->lock);
    }
    return ip;
  }

  // Allocate fresh inode cache slot.
  ip = inode::alloc(dev, inum);
  if (!ip)
    panic("iget: should throw_bad_alloc()");

  // Lock the inode
  ip->busy = true;
  ip->readbusy = 1;

  if (!ins->insert(make_pair(ip->dev, ip->inum), ip.get())) {
    iunlock(ip);
    // reference counting will clean up memory allocation.
    goto retry;
  }

  ip->init();
  iunlock(ip);
  return ip;
}

sref<inode>
inode::alloc(u32 dev, u32 inum)
{
  sref<inode> ip = sref<inode>::transfer(new inode(dev, inum));
  if (!ip)
    return sref<inode>();

  snprintf(ip->lockname, sizeof(ip->lockname), "cv:ino:%d", ip->inum);
  ip->lock = spinlock(ip->lockname+3, LOCKSTAT_FS);
  ip->cv = condvar(ip->lockname);
  return ip;
}

void
inode::init(void)
{
  scoped_gc_epoch e;
  sref<buf> bp = buf::get(dev, IBLOCK(inum));
  auto copy = bp->read();
  const dinode *dip = (const struct dinode*)copy->data + inum%IPB;

  type = dip->type;
  major = dip->major;
  minor = dip->minor;
  nlink_ = dip->nlink;
  size = dip->size;
  gen = dip->gen;
  memmove(addrs, dip->addrs, sizeof(addrs));

  if (nlink_ > 0)
    inc();

  // Perform another increment. This is decremented from mfs_interface::
  // free_inode(), possibly from the deferred inode reclamation path. This is
  // to help keep the inode around until all the open file descriptors of this
  // file have been closed, even if that happens after the last unlink().
  inc();

  valid.store(true);
}

// Caller must hold ilock() for write, if inode is accessible by multiple
// threads.
void
inode::link(void)
{
  if (++nlink_ == 1) {
    // A non-zero nlink_ holds a reference to the inode
    inc();
  }
}

// Caller must hold ilock() for write, if inode is accessible by multiple
// threads.
void
inode::unlink(void)
{
  if (--nlink_ == 0) {
    // This should never be the last reference..
    dec();
  }
}

short
inode::nlink(void)
{
  return nlink_;
}

void
inode::onzero(void)
{
  acquire(&lock);

  if (busy || readbusy)
    panic("inode::onzero() : inode is busy (locked)\n");

  if (!valid)
    panic("inode::onzero() : inode's valid flag is false\n");

  busy = true;
  readbusy++;

  release(&lock);

  ins->remove(make_pair(dev, inum));
  gc_delayed(this);
  return;
}

// Lock the given inode, for write if @lock_type == WRITELOCK, and for read
// otherwise.
void
ilock(sref<inode> ip, int lock_type)
{
  if (!ip)
    panic("ilock(): illegal inode pointer\n");

  acquire(&ip->lock);
  if (lock_type == WRITELOCK) {
    while (ip->busy || ip->readbusy)
      ip->cv.sleep(&ip->lock);
    ip->busy = true;
  } else {
    while (ip->busy)
      ip->cv.sleep(&ip->lock);
  }
  ip->readbusy++;
  release(&ip->lock);

  if (!ip->valid)
    panic("ilock(): inode's valid flag is false\n");
}

// Unlock the given inode.
void
iunlock(sref<inode> ip)
{
  if (!ip)
    panic("iunlock(): illegal inode pointer\n");

  if (!ip->readbusy && !ip->busy)
    panic("iunlock(): inode not locked\n");

  acquire(&ip->lock);
  --ip->readbusy;
  ip->busy = false;
  ip->cv.wake_all();
  release(&ip->lock);
}

// Inode contents
//
// The contents (data) associated with each inode is stored in a sequence of
// blocks on the disk.  The first NDIRECT blocks are listed in ip->addrs[].
// The next NINDIRECT blocks are listed in the block ip->addrs[NDIRECT].
// The next NINDIRECT^2 blocks are doubly-indirect from ip->addrs[NDIRECT+1].

// Return the disk block address of the nth block in inode ip. If there is no
// such block, bmap allocates one. The caller must hold ilock() for write if
// invoking bmap() from writei().
static u32
bmap(sref<inode> ip, u32 bn, transaction *trans = NULL, bool zero_on_alloc = false,
     bool lazy_trans_update = false)
{
  scoped_gc_epoch e;
  bool skip_disk_read = false;
  u32* ap;

  if (bn < NDIRECT) {
    if (ip->addrs[bn] == 0)
      ip->addrs[bn] = balloc(ip->dev, trans, zero_on_alloc);

    return ip->addrs[bn];
  }
  bn -= NDIRECT;

  if (bn < NINDIRECT) {
    if (ip->addrs[NDIRECT] == 0) {
      ip->addrs[NDIRECT] = balloc(ip->dev, trans, true);
      // We allocated the block just now. So need to read it from the disk.
      skip_disk_read = true;
    }

    sref<buf> bp = buf::get(ip->dev, ip->addrs[NDIRECT], skip_disk_read);
    skip_disk_read = false;
    auto locked = bp->write();
    ap = (u32 *)locked->data;

    if (ap[bn] == 0) {
      ap[bn] = balloc(ip->dev, trans, zero_on_alloc);
      if (trans) {
        if (lazy_trans_update)
          bp->add_blocknum_to_transaction(trans);
        else
          bp->add_to_transaction(trans);
      }
    }

    return ap[bn];
  }
  bn -= NINDIRECT;

  if (bn >= NINDIRECT * NINDIRECT)
    panic("bmap: %d out of range", bn);

  if (ip->addrs[NDIRECT+1] == 0) {
    ip->addrs[NDIRECT+1] = balloc(ip->dev, trans, true);
    // We allocated the block just now. So need to read it from the disk.
    skip_disk_read = true;
  }

  // First-level doubly-indirect block
  sref<buf> fp = buf::get(ip->dev, ip->addrs[NDIRECT+1], skip_disk_read);
  skip_disk_read = false;
  auto flocked = fp->write();
  ap = (u32 *)flocked->data;

  if (ap[bn / NINDIRECT] == 0) {
    ap[bn / NINDIRECT] = balloc(ip->dev, trans, true);
    // We allocated the block just now. So need to read it from the disk.
    skip_disk_read = true;

    if (trans) {
      if (lazy_trans_update)
        fp->add_blocknum_to_transaction(trans);
      else
        fp->add_to_transaction(trans);
    }
  }

  // Second-level doubly-indirect block
  sref<buf> sp = buf::get(ip->dev, ap[bn / NINDIRECT], skip_disk_read);
  skip_disk_read = false;
  auto slocked = sp->write();
  ap = (u32 *)slocked->data;

  if (ap[bn % NINDIRECT] == 0) {
    ap[bn % NINDIRECT] = balloc(ip->dev, trans, zero_on_alloc);
    if (trans) {
      if (lazy_trans_update)
        sp->add_blocknum_to_transaction(trans);
      else
        sp->add_to_transaction(trans);
    }
  }

  return ap[bn % NINDIRECT];
}

// Caller must hold ilock for write. The caller must also arrange to invoke
// iupdate() when suitable, to flush the new inode size to the disk.
void
itrunc(sref<inode> ip, u32 offset, transaction *trans)
{
  scoped_gc_epoch e;

  if (ip->size <= offset || offset >= MAXFILE*BSIZE)
    return;

  // Wipe out everything from bn (inclusive) till the end of the file.
  // After itrunc() returns, appends will occur at 'offset'.
  u32 bn = BLOCKROUNDUP(offset);

  enum {
    DIRECT_BLOCKS = 1,
    INDIRECT_BLOCKS,
    DBL_INDIRECT_BLOCKS,
  };

  u32 start_stage = DIRECT_BLOCKS, start_index = 0;

  if (bn < NDIRECT) {
    start_stage = DIRECT_BLOCKS;
    start_index = bn;
  } else if (bn < NDIRECT + NINDIRECT) {
    start_stage = INDIRECT_BLOCKS;
    start_index = bn - NDIRECT;
  } else if (bn < NDIRECT + NINDIRECT + NINDIRECT*NINDIRECT) {
    start_stage = DBL_INDIRECT_BLOCKS;
    start_index = bn - NDIRECT - NINDIRECT;
  }

  switch (start_stage) {
  case DIRECT_BLOCKS:

    for (u32 i = start_index; i < NDIRECT; i++) {
      if (!ip->addrs[i])
        break;
      bfree(ip->dev, ip->addrs[i], trans, true);
      ip->addrs[i] = 0;
    }
    start_index = 0; // Fall through to next stage.

  case INDIRECT_BLOCKS:

    if (!ip->addrs[NDIRECT])
      break; // No more blocks to delete.

    {
      sref<buf> bp = buf::get(ip->dev, ip->addrs[NDIRECT]);
      auto locked = bp->write();
      u32 *ap = (u32 *)locked->data;

      for (u32 i = start_index; i < NINDIRECT; i++) {
        if (!ap[i])
          break;

        bfree(ip->dev, ap[i], trans, true);
        ap[i] = 0;
      }

      if (start_index != 0)
        bp->add_to_transaction(trans);
    }

    if (start_index == 0) {
      bfree(ip->dev, ip->addrs[NDIRECT], trans, true);
      ip->addrs[NDIRECT] = 0;
    }

    start_index = 0; // Fall through to next stage.

  case DBL_INDIRECT_BLOCKS:

    if (!ip->addrs[NDIRECT+1])
      break;

    {
      sref<buf> bp1 = buf::get(ip->dev, ip->addrs[NDIRECT+1]);
      auto locked1 = bp1->write();
      u32 *ap1 = (u32 *)locked1->data;
      u32 begin = start_index;

      for (u32 i = begin / NINDIRECT; i < NINDIRECT; i++) {
        if (!ap1[i])
          break;

        {
          sref<buf> bp2 = buf::get(ip->dev, ap1[i]);
          auto locked2 = bp2->write();
          u32 *ap2 = (u32 *)locked2->data;

          for (u32 j = begin % NINDIRECT; j < NINDIRECT; j++) {
            if (!ap2[j])
              break;

            bfree(ip->dev, ap2[j], trans, true);
            ap2[j] = 0;
          }

          if (!ap2[0])
            bp2->add_to_transaction(trans);
        }

        if (begin % NINDIRECT == 0) {
          bfree(ip->dev, ap1[i], trans, true);
          ap1[i] = 0;
        }

        // Reset 'begin' after the first run through the nested loop, to ensure
        // that its subsequent executions will process all the entries.
        begin = 0;
      }

      if (start_index != 0)
        bp1->add_to_transaction(trans);
    }

    if (start_index == 0) {
      bfree(ip->dev, ip->addrs[NDIRECT+1], trans, true);
      ip->addrs[NDIRECT+1] = 0;
    }
  }

  // Final correctness check for the most common case:
  if (offset == 0) {
    for (u32 i = 0; i < NDIRECT + 2; i++)
      assert(ip->addrs[i] == 0);
  }

  ip->size = offset;
}

// Drop the (clean) buffer-cache blocks associated with this file.
// Caller must hold ilock for read.
void
drop_bufcache(sref<inode> ip)
{
  scoped_gc_epoch e;

  for (int i = 0; i < NDIRECT; i++) {
    if (ip->addrs[i])
      buf::put(ip->dev, ip->addrs[i]);
  }

  // Note: If the indirect or doubly indirect blocks are themselves not in the
  // bufcache, none of the data-blocks they point to will be in the bufcache
  // either. So check that first! Don't read blocks from the disk into the
  // bufcache just to throw them out!

  if (ip->addrs[NDIRECT] && buf::in_bufcache(ip->dev, ip->addrs[NDIRECT])) {
    sref<buf> bp = buf::get(ip->dev, ip->addrs[NDIRECT]);
    auto copy = bp->read();
    u32 *a = (u32*)copy->data;
    for (int i = 0; i < NINDIRECT; i++) {
      if (a[i])
        buf::put(ip->dev, a[i]);
    }
    // Drop the indirect block.
    buf::put(ip->dev, ip->addrs[NDIRECT]);
  }

  if (ip->addrs[NDIRECT+1] && buf::in_bufcache(ip->dev, ip->addrs[NDIRECT+1])) {
    sref<buf> bp1 = buf::get(ip->dev, ip->addrs[NDIRECT+1]);
    auto copy1 = bp1->read();
    u32 *a1 = (u32*)copy1->data;

    for (int i = 0; i < NINDIRECT; i++) {
      if (a1[i] && buf::in_bufcache(ip->dev, a1[i])) {
        sref<buf> bp2 = buf::get(ip->dev, a1[i]);
        auto copy2 = bp2->read();
        u32 *a2 = (u32*)copy2->data;
        for (int j = 0; j < NINDIRECT; j++) {
          if (a2[j])
            buf::put(ip->dev, a2[j]);
        }
        // Drop the second-level doubly-indirect block.
        buf::put(ip->dev, a1[i]);
      }
    }

    // Drop the first-level doubly-indirect block.
    buf::put(ip->dev, ip->addrs[NDIRECT+1]);
  }
}

// Read data from the inode. Called when the inode's data blocks are not cached
// in the page-cache (MemFS) and hence have to be read fresh from the disk itself.
//
// Locking protocol: None. The caller doesn't need to hold ilock for read,
// because readi() and writei() can never be concurrent on the same inode, asking
// to read and write the same set of blocks. Here's why: writei() is only invoked
// in the fsync path, to flush out dirty data from the page-cache to the
// file/dir on the disk (via the bufcache). When an fsync() [and hence writei()]
// is in progress, if a concurrent read() on the file asks for dirty blocks, it
// will get fulfilled from the page-cache itself [i.e., readm() in MemFS] without
// turning into a call to readi(). Instead, if the read() asks for clean blocks
// of the file, then it can safely read from the bufcache via readi() because
// the writei() (in the fsync path) doesn't modify any clean blocks. Thus, even
// if we have concurrent calls to readi() and writei() on the same inode, they
// will touch a mutually exclusive set of blocks, which implies that we don't
// need any synchronization between them.
int
readi(sref<inode> ip, char *dst, u32 off, u32 n)
{
  scoped_gc_epoch e;

  u32 tot, m;
  sref<buf> bp;

  if (ip->type == T_DEV)
    return -1;

  if (off > ip->size || off + n < off)
    return -1;
  if (off + n > ip->size)
    n = ip->size - off;

  for (tot=0; tot<n; tot+=m, off+=m, dst+=m) {
    try {
      bp = buf::get(ip->dev, bmap(ip, off/BSIZE, NULL, true));
    } catch (out_of_blocks& e) {
      // Read operations should never cause out-of-blocks conditions
      panic("readi: out of blocks");
    }
    m = std::min(n - tot, BSIZE - off%BSIZE);

    auto copy = bp->read();
    memmove(dst, copy->data + off%BSIZE, m);
  }
  return n;
}

// Write data to the inode. Called in the fsync() path to flush dirty data from
// the page-cache (MemFS) to the inode's data blocks on the disk via the
// bufcache.
//
// The modified blocks are written to the disk directly if @writeback == true,
// and logged in the transaction otherwise.
//
// Locking protocol: The caller must hold ilock for write.
//
// Strictly speaking, this is unnecessary because concurrent calls to fsync()
// on the same inode are serialized at the fsync() call itself (using per-mnode
// locks). But we enforce this locking protocol here anyway to maintain writei()'s
// correctness guarantees independent of fsync()'s concurrency strategy.
int
writei(sref<inode> ip, const char *src, u32 off, u32 n, transaction *trans,
       bool writeback, bool lazy_trans_update, bool dont_cache)
{
  scoped_gc_epoch e;

  int tot, m;
  sref<buf> bp;
  u32 blocknum;

  if (ip->type == T_DEV)
    return -1;

  //if (off > ip->size || off + n < off)
  if (off + n < off)
    return -1;

  if (off + n > MAXFILE*BSIZE)
    n = MAXFILE*BSIZE - off;

  for (tot=0; tot<n; tot+=m, off+=m, src+=m) {

    bool skip_disk_read = false;
    m = std::min(n - tot, BSIZE - off%BSIZE);

    try {

      // Skip reading the block from disk if we are going to overwrite the
      // entire block anyway.
      if (off % BSIZE == 0 && m == BSIZE)
        skip_disk_read = true;

      blocknum = bmap(ip, off/BSIZE, trans, !skip_disk_read, lazy_trans_update);

      // Don't cache file data pages in the bufcache.
      if (!dont_cache)
        bp = buf::get(ip->dev, blocknum, skip_disk_read);

    } catch (out_of_blocks& e) {
      console.println("writei: out of blocks");
      // If we haven't written anything, return an error
      if (tot == 0)
        tot = -1;
      break;
    }

    if (!dont_cache) {
      auto locked = bp->write();
      memmove(locked->data + off%BSIZE, src, m);

      // If adding the block to the transaction, we need to copy the contents
      // of the block with the write-lock held, so that we add *this* particular
      // version of the block-contents to the transaction. Also, this placement
      // helps ensure that the buf is marked clean at the right moment.
      if (!writeback && trans) {
        if (lazy_trans_update)
          bp->add_blocknum_to_transaction(trans);
        else
          bp->add_to_transaction(trans);
      }
    }

    // Use the transaction's private block layer to accumulate the writes and
    // flush them out in bigger chunks using scatter-gather I/O.
    if (writeback)
      trans->write_block(ip->dev, src, blocknum);
  }
  // Don't update inode yet. Wait till all the pages have been written to and then
  // call update_size to update the inode just once.

  return tot;
}

void
update_size(sref<inode> ip, u32 size, transaction *trans)
{
  ip->size = size;
  iupdate(ip, trans);
}

// Directories

void
dir_init(sref<inode> dp)
{
  scoped_gc_epoch e;

  if (dp->dir)
    return;

  if (dp->type != T_DIR)
    panic("dir_init: inode is not a directory\n");

  dp->dir = new dir_entries(NDIR_ENTRIES_PRIME);
  u32 dir_offset = 0;

  for (u32 off = 0; off < dp->size; off += BSIZE) {
    assert(dir_offset == off);
    sref<buf> bp;
    try {
      bp = buf::get(dp->dev, bmap(dp, off / BSIZE, NULL, true));
    } catch (out_of_blocks& e) {
      // Read operations should never cause out-of-blocks conditions
      panic("dir_init: out of blocks");
    }

    auto copy = bp->read();
    for (const struct dirent *de = (const struct dirent *) copy->data;
	 de < (const struct dirent *) (copy->data + BSIZE);
	 de++) {

      if (de->inum) {
        dir_entry_info de_info(de->inum, dir_offset);
        dp->dir->insert(strbuf<DIRSIZ>(de->name), de_info);
      }

      dir_offset += sizeof(*de);
    }
  }

  dp->dir_offset = dir_offset;
}

// Caller must hold ilock for write.
void
dir_flush_entry(sref<inode> dp, const char *name, transaction *trans)
{
  if (!dp->dir)
    return;

  dir_entry_info de_info;
  dp->dir->lookup(strbuf<DIRSIZ>(name), &de_info);

  struct dirent de;
  strncpy(de.name, name, DIRSIZ);
  de.inum = de_info.inum_;

  if (writei(dp, (char *)&de, de_info.offset_, sizeof(de), trans) != sizeof(de))
    panic("dir_flush_entry");

  if (dp->size < de_info.offset_ + sizeof(de)) {
    dp->size = de_info.offset_ + sizeof(de);
  }

  iupdate(dp, trans);
}

// Look for a directory entry in a directory.
sref<inode>
dirlookup(sref<inode> dp, char *name)
{
  dir_init(dp);

  dir_entry_info de_info;
  dp->dir->lookup(strbuf<DIRSIZ>(name), &de_info);

  if (de_info.inum_ == 0)
    return sref<inode>();
  return iget(dp->dev, de_info.inum_);
}

// Write a new directory entry (name, inum) into the directory dp.
int
dirlink(sref<inode> dp, const char *name, u32 inum, bool inc_link,
        transaction *trans)
{
  bool ip_updated = false;
  sref<inode> ip;
  dir_init(dp);

  dir_entry_info de_info(inum, dp->dir_offset);

  if (!dp->dir->insert(strbuf<DIRSIZ>(name), de_info))
    return -1;

  dp->dir_offset += sizeof(struct dirent);

  // If adding the ".." link in a directory, don't change *any* link counts.
  if (strncmp(name, "..", DIRSIZ) != 0) {
    ip = iget(1, inum);
    if (ip) {
      ip->link();
      ip_updated = true;
    }

    if (inc_link)
      dp->link();
  }

  dir_flush_entry(dp, name, trans);

  // Update the on-disk link count of the inode being linked.
  if (ip_updated)
    iupdate(ip, trans);
  return 0;
}

// Remove a directory entry (name, inum) from the directory dp.
int
dirunlink(sref<inode> dp, const char *name, u32 inum, bool dec_link,
          transaction *trans)
{
  bool ip_updated = false;
  sref<inode> ip;
  dir_init(dp);

  dir_entry_info de_info;
  dp->dir->lookup(strbuf<DIRSIZ>(name), &de_info);

  if (!dp->dir->remove(strbuf<DIRSIZ>(name)))
    return -1;

  de_info.inum_ = 0;
  if (!dp->dir->insert(strbuf<DIRSIZ>(name), de_info))
    return -1;

  // If removing the ".." link in a directory, don't change *any* link counts.
  if (strncmp(name, "..", DIRSIZ) != 0) {
    ip = iget(1, inum);
    if (ip) {
      ip->unlink();
      ip_updated = true;
    }

    if (dec_link)
      dp->unlink();
  }

  dir_flush_entry(dp, name, trans);
  dp->dir->remove(strbuf<DIRSIZ>(name));

  // Update the on-disk link count of the inode being unlinked.
  if (ip_updated)
    iupdate(ip, trans);
  return 0;
}

// Paths

// Copy the next path element from path into name.
// Update the pointer to the element following the copied one.
// The returned path has no leading slashes,
// so the caller can check *path=='\0' to see if the name is the last one.
//
// If copied into name, return 1.
// If no name to remove, return 0.
// If the name is longer than DIRSIZ, return -1;
//
// Examples:
//   skipelem("a/bb/c", name) = "bb/c", setting name = "a"
//   skipelem("///a//bb", name) = "bb", setting name = "a"
//   skipelem("a", name) = "", setting name = "a"
//   skipelem("", name) = skipelem("////", name) = 0
//
static int
skipelem(const char **rpath, char *name)
{
  const char *path = *rpath;
  const char *s;
  int len;

  while (*path == '/')
    path++;
  if (*path == 0)
    return 0;
  s = path;
  while (*path != '/' && *path != 0)
    path++;
  len = path - s;
  if (len > DIRSIZ) {
    cprintf("Error: Path component longer than DIRSIZ"
            " (%d characters)\n", DIRSIZ);
    return -1;
  } else {
    memmove(name, s, len);
    if (len < DIRSIZ)
      name[len] = 0;
  }
  while (*path == '/')
    path++;
  *rpath = path;
  return 1;
}

// Look up and return the inode for a path name.
// If parent != 0, return the inode for the parent and copy the final
// path element into name, which must have room for DIRSIZ bytes.
static sref<inode>
namex(sref<inode> cwd, const char *path, int nameiparent, char *name)
{
  // Assumes caller is holding a gc_epoch

  sref<inode> ip;
  sref<inode> next;
  int r;

  if (*path == '/')
    ip = the_root;
  else
    ip = cwd;

  while ((r = skipelem(&path, name)) == 1) {
    // XXX Doing this here requires some annoying reasoning about all
    // of the callers of namei/nameiparent.  Also, since the abstract
    // scope is implicit, it might be wrong (or non-existent) and
    // documenting the abstract object sets of each scope becomes
    // difficult and probably unmaintainable.  We have to compute this
    // information here because it's the only place that's canonical.
    // Maybe this should return the set of inodes traversed and let
    // the caller declare the variables?  Would it help for the caller
    // to pass in an abstract scope?
    mtreadavar("inode:%x.%x", ip->dev, ip->inum);
    if (ip->type == 0)
      panic("namex");
    if (ip->type != T_DIR)
      return sref<inode>();
    if (nameiparent && *path == '\0') {
      // Stop one level early.
      return ip;
    }

    if ((next = dirlookup(ip, name)) == 0)
      return sref<inode>();
    ip = next;
  }

  if (r == -1 || nameiparent)
    return sref<inode>();

  return ip;
}

sref<inode>
namei(sref<inode> cwd, const char *path)
{
  // Assumes caller is holding a gc_epoch
  char name[DIRSIZ];
  return namex(cwd, path, 0, name);
}

sref<inode>
nameiparent(sref<inode> cwd, const char *path, char *name)
{
  // Assumes caller is holding a gc_epoch
  return namex(cwd, path, 1, name);
}
