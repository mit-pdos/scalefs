#include "types.h"
#include "kernel.hh"
#include "fs.h"
#include "file.hh"
#include "mnode.hh"
#include "mfs.hh"
#include "scalefs.hh"
#include "kstream.hh"
#include "major.h"


mfs_interface::mfs_interface()
{
  for (int cpu = 0; cpu < NCPU; cpu++)
    fs_journal[cpu] = new journal();

  inum_to_mnum = new chainhash<u64, u64>(NINODES_PRIME);
  mnum_to_inum = new chainhash<u64, u64>(NINODES_PRIME);
  mnum_to_lock = new chainhash<u64, sleeplock*>(NINODES_PRIME);
  mnum_to_name = new chainhash<u64, strbuf<DIRSIZ>>(NINODES_PRIME); // Debug
  metadata_log_htab = new chainhash<u64, mfs_logical_log*>(NINODES_PRIME);
  blocknum_to_queue = new chainhash<u32, tx_queue_info>(NINODEBITMAP_BLKS_PRIME);
}

bool
mfs_interface::mnum_name_insert(u64 mnum, const strbuf<DIRSIZ>& name)
{
#if DEBUG
  return mnum_to_name->insert(mnum, name);
#else
  return true;
#endif
}

bool
mfs_interface::mnum_name_lookup(u64 mnum, strbuf<DIRSIZ> *nameptr)
{
  return mnum_to_name->lookup(mnum, nameptr);
}

bool
mfs_interface::inum_lookup(u64 mnum, u64 *inumptr)
{
  return mnum_to_inum->lookup(mnum, inumptr);
}

sref<mnode>
mfs_interface::mnode_lookup(u64 inum, u64 *mnumptr)
{
  if (inum_to_mnum->lookup(inum, mnumptr))
    return root_fs->mget(*mnumptr);
  return sref<mnode>();
}

void
mfs_interface::alloc_mnode_lock(u64 mnum)
{
  mnum_to_lock->insert(mnum, new sleeplock());
}

void
mfs_interface::free_mnode_lock(u64 mnum)
{
  sleeplock *mnode_lock;
  if (mnum_to_lock->lookup(mnum, &mnode_lock)) {
    mnum_to_lock->remove(mnum);
    delete mnode_lock;
  }
}

void
mfs_interface::alloc_metadata_log(u64 mnum)
{
  mfs_logical_log *mfs_log = new mfs_logical_log(mnum);

  for (int cpu = 0; cpu < NCPU; cpu++) {
    scoped_acquire a(&mfs_log->link_lock[cpu]);
    mfs_log->link_count[cpu] = 0;
  }

  metadata_log_htab->insert(mnum, mfs_log);
}

void
mfs_interface::free_metadata_log(u64 mnum)
{
  mfs_logical_log *mfs_log;
  if (metadata_log_htab->lookup(mnum, &mfs_log)) {
    metadata_log_htab->remove(mnum);
    delete mfs_log;
  }
}

// Returns an sref to an inode if mnum is mapped to one.
sref<inode>
mfs_interface::get_inode(u64 mnum, const char *str)
{
  u64 inum = 0;

  if (!inum_lookup(mnum, &inum))
    panic("%s: Inode mapping for mnode# %ld does not exist", str, mnum);

  return iget(1, inum);
}

// Initializes the size of an mfile to the on-disk file size. This helps the
// mfile distinguish between when a file page has to be demand-loaded from the
// disk and when a new page has to be allocated. Called the first time the mfile
// is referred to.
void
mfs_interface::initialize_file(sref<mnode> m)
{
  scoped_gc_epoch e;
  sref<inode> i = get_inode(m->mnum_, "initialize_file");

  auto resizer = m->as_file()->write_size();
  resizer.initialize_from_disk(i->size);
}

// Reads in a file page from the disk.
int
mfs_interface::load_file_page(u64 mfile_mnum, char *p, size_t pos,
		              size_t nbytes)
{
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_mnum, "load_file_page");
  return readi(i, p, pos, nbytes);
}

// Reads the on-disk file size.
u64
mfs_interface::get_file_size(u64 mfile_mnum)
{
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_mnum, "get_file_size");
  return i->size;
}

// Updates the file size on the disk.
void
mfs_interface::update_file_size(u64 mfile_mnum, u32 size, transaction *tr)
{
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_mnum, "update_file_size");
  update_size(i, size, tr);
}

sref<inode>
mfs_interface::prepare_sync_file_pages(u64 mfile_mnum, transaction *tr)
{
  scoped_gc_epoch e;
  sref<inode> ip = get_inode(mfile_mnum, "sync_file_page");

  std::vector<u64> inum_list;
  inum_list.push_back(ip->inum);

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock().
  acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);

  ilock(ip, WRITELOCK);
  return ip;
}

// Flushes out the contents of an in-memory file page to the disk.
int
mfs_interface::sync_file_page(sref<inode> ip, char *p, size_t pos,
                              size_t nbytes, transaction *tr)
{
  scoped_gc_epoch e;
  // writeback = true, lazy_trans_update = true.
  return writei(ip, p, pos, nbytes, tr, true, true);
}

void
mfs_interface::finish_sync_file_pages(sref<inode> ip, transaction *tr)
{
  scoped_gc_epoch e;

  // Make sure that there are no pending writes in the block-queue.
  tr->flush_block_queue();
  tr->add_dirty_blocks_lazy();
  iunlock(ip);
}

// Truncates a file on disk to the specified size (offset).
void
mfs_interface::truncate_file(u64 mfile_mnum, u32 offset, transaction *tr)
{
  scoped_gc_epoch e;

  sref<inode> ip = get_inode(mfile_mnum, "truncate_file");
  ilock(ip, WRITELOCK);
  itrunc(ip, offset, tr);
  iunlock(ip);

  sref<mnode> m = root_fs->mget(mfile_mnum);
  if (m)
    m->as_file()->remove_pgtable_mappings(offset);
}

// Returns an inode locked for write, on success.
sref<inode>
mfs_interface::alloc_inode_for_mnode(u64 mnum, u8 type)
{
  sref<inode> ip;
  sleeplock *mnode_lock;
  assert(mnum_to_lock->lookup(mnum, &mnode_lock));
  auto lk = mnode_lock->guard();

  u64 inum;
  if (inum_lookup(mnum, &inum)) {
    ip = iget(1, inum);
    ilock(ip, WRITELOCK);
    return ip;
  }

  // ialloc() returns a locked inode.
  ip = ialloc(1, type);
  inum_to_mnum->insert(ip->inum, mnum);
  mnum_to_inum->insert(mnum, ip->inum);
  return ip;
}

// Creates a new file on the disk if an mnode (mfile) does not have a
// corresponding inode mapping.
void
mfs_interface::create_file(u64 mnum, u8 type, transaction *tr)
{
  sref<inode> ip = alloc_inode_for_mnode(mnum, type);
  iunlock(ip);

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock().
  std::vector<u64> inum_list;
  inum_list.push_back(ip->inum);
  acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);

  // Buffer-cache updates start here.
  ilock(ip, WRITELOCK);
  iupdate(ip, tr);
  iunlock(ip);
}

// Creates a new directory on the disk if an mnode (mdir) does not have a
// corresponding inode mapping. This does not change the link counts of the
// parent or the newly created sub-directory. (That is postponed until the
// sub-directory is actually linked into the parent.)
void
mfs_interface::create_dir(u64 mnum, u64 parent_mnum, u8 type, transaction *tr)
{
  u64 parent_inum = 0;
  sref<inode> parent_ip, subdir_ip;

  // The new sub-directory needs to be initialized with the ".." link, pointing
  // to its parent's inode number.
  if (!inum_lookup(parent_mnum, &parent_inum)) {
    parent_ip = alloc_inode_for_mnode(parent_mnum, mnode::types::dir);
    iunlock(parent_ip);
    parent_inum = parent_ip->inum;
  }

  subdir_ip = alloc_inode_for_mnode(mnum, type);
  iunlock(subdir_ip);

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock().
  std::vector<u64> inum_list;
  inum_list.push_back(parent_inum);
  inum_list.push_back(subdir_ip->inum);
  acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);

  // Buffer-cache updates start here.
  ilock(subdir_ip, WRITELOCK);
  dirlink(subdir_ip, "..", parent_inum, false, tr);

  // Flush parent inode too, if it was newly created above.
  if (parent_ip) {
    ilock(parent_ip, WRITELOCK);
    iupdate(parent_ip, tr);
    iunlock(parent_ip);
  }

  iunlock(subdir_ip);
}

// Creates a directory entry for a name that exists in the in-memory
// representation but not on the disk.
void
mfs_interface::add_dir_entry(u64 mdir_mnum, char *name, u64 dirent_mnum,
		             u8 type, transaction *tr, bool rename_link)
{
  sref<inode> mdir_ip = get_inode(mdir_mnum, "add_dir_entry");

  // If add_dir_entry() was invoked in the rename-link path, don't acquire the
  // inode-bitmap locks.
  bool acquire_locks = rename_link ? false : true;

  u64 dirent_inum = 0;
  assert(inum_lookup(dirent_mnum, &dirent_inum));

  // Check if the directory entry already exists.
  sref<inode> ip = dirlookup(mdir_ip, name);

  if (ip) {
    if (ip->inum == dirent_inum)
      return;

    // FIXME: Restore the call to remove_dir_entry; with that, the buffer-cache
    // updates will actually start at this point. So fix the lock ordering for
    // the inode-block locks.
#if 0
    // The name now refers to a different inode. Unlink the old one to make
    // way for a new directory entry for this mapping.
    remove_dir_entry(mdir_mnum, name, tr);
#endif
  }

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock().
  if (acquire_locks) {
    std::vector<u64> inum_list;
    inum_list.push_back(mdir_ip->inum);
    inum_list.push_back(dirent_inum);
    acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);
  }

  sref<inode> dirent_ip = iget(1, dirent_inum);

  // Buffer-cache updates start here.
  ilock(mdir_ip, WRITELOCK);
  ilock(dirent_ip, WRITELOCK);
  dirlink(mdir_ip, name, dirent_inum, (type == mnode::types::dir)?true:false, tr);
  iunlock(dirent_ip);
  iunlock(mdir_ip);
}

// Deletes directory entries (from the disk) which no longer exist in the mdir.
// The file/directory names that are present in the mdir are specified in names_vec.
void
mfs_interface::remove_dir_entry(u64 mdir_mnum, char* name, transaction *tr,
                                bool rename_unlink)
{
  sref<inode> mdir_ip = get_inode(mdir_mnum, "remove_dir_entry");
  sref<inode> target = dirlookup(mdir_ip, name);
  if (!target)
    return;

  // If remove_dir_entry() was invoked in the rename-unlink path, don't acquire
  // the inode-bitmap locks.
  bool acquire_locks = rename_unlink ? false : true;

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock().
  if (acquire_locks) {
    std::vector<u64> inum_list;
    inum_list.push_back(mdir_ip->inum);
    inum_list.push_back(target->inum);
    acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);
  }

  ilock(mdir_ip, WRITELOCK);
  ilock(target, WRITELOCK);
  // Buffer-cache updates start here.
  if (target->type == T_DIR)
    dirunlink(mdir_ip, name, target->inum, true, tr);
  else
    dirunlink(mdir_ip, name, target->inum, false, tr);
  iunlock(target);
  iunlock(mdir_ip);

  assert(target->nlink() >= 0);

  // The link and unlink are atomic for renames; and put together, the don't
  // affect the reachability or the overall link-count of an inode.
  if (rename_unlink)
    return;

  u64 mnum;
  assert(inum_to_mnum->lookup(target->inum, &mnum));
  dec_mfslog_linkcount(mnum);

  // Now check what the global link-count of the inode has to say. If it is
  // zero, it means that there are no pending links to this inode waiting to be
  // flushed from other directories/oplogs. So this unlink really removes the
  // last link to the inode, making it safe to delete it from the disk (as long
  // as we don't have any open file descriptors referring to that inode).
  if (!get_mfslog_linkcount(mnum))
    delete_mnum_inode_safe(mnum, tr);
}

// Delete the inode corresponding to the mnum and its file-contents from the
// disk, if there are no open file descriptors referring to that inode.
void
mfs_interface::delete_mnum_inode_safe(u64 mnum, transaction *tr,
                                      bool acquire_locks, bool mnode_dying)
{
  u64 inum = 0;
  assert(inum_lookup(mnum, &inum));

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock() [via __delete_mnum_inode()].
  if (acquire_locks) {
    std::vector<u64> inum_list;
    inum_list.push_back(inum);
    acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);
  }

  // Ideally, we shouldn't need this extra check, as this scenario should
  // automatically go to the else condition below; but for some mysterious
  // reason, that doesn't seem to work reliably. So we have this blunt check
  // to make sure we absolutely get this right.
  if (mnode_dying) {
    // mnode_dying == true indicates that the mnode has reached onzero().
    // So it is safe to delete its inode from the disk.
    __delete_mnum_inode(mnum, tr);
    return;
  }

  sref<mnode> m = root_fs->mget(mnum);
  if (m) {
    // It looks like userspace still has open file descriptors referring to
    // this mnode, so it is not safe to delete its on-disk inode just yet.
    // So mark it for deletion and postpone it until this mnode's onzero()
    // function is invoked.
    m->mark_inode_for_deletion();
  } else {
    // The mnode is gone (which also implies that all its open file
    // descriptors have been closed as well). So it is safe to delete its
    // inode from the disk.
    __delete_mnum_inode(mnum, tr);
  }
}

// Deletes the inode corresponding to the mnum and its file-contents from the
// disk.
void
mfs_interface::__delete_mnum_inode(u64 mnum, transaction *tr)
{
  u64 inum = 0;
  if (!inum_lookup(mnum, &inum))
    return; // Somebody already deleted this inode.

  // If we are deleting the inode corresponding to a file mnode due to
  // absorption, we must ensure that the mnode is marked clean before deleting
  // the inode. Otherwise, a later call to sync_file_page() will crash as it
  // won't find the inode corresponding to the dirty file mnode it is trying to
  // flush.
  sref<mnode> m = root_fs->mget(mnum);
  if (m && m->is_dirty())
    m->dirty(false);

  sref<inode> ip = iget(1, inum);

  ilock(ip, WRITELOCK);
  itrunc(ip, 0, tr);
  iunlock(ip);

  mnum_to_inum->remove(mnum);
  inum_to_mnum->remove(ip->inum);
  free_inode(ip, tr);
}

// Initializes the mdir the first time it is referred to. Populates directory
// entries from the disk.
void
mfs_interface::initialize_dir(sref<mnode> m)
{
  scoped_gc_epoch e;
  sref<inode> i = get_inode(m->mnum_, "initialize_dir");
  load_dir(i, m);
}

lock_guard<sleeplock>
mfs_interface::metadata_op_lockguard(u64 mnum, int cpu)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  return std::move(mfs_log->get_tsc_lock_guard(cpu));
}

// Both metadata_op_start() and metadata_op_end() must be invoked while keeping
// the lock-guard returned by metadata_op_lockguard() alive. They must both be
// invoked in the same critical section, without releasing the lock in between.

void
mfs_interface::metadata_op_start(u64 mnum, int cpu, u64 tsc_val)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  mfs_log->update_start_tsc(cpu, tsc_val);
}

void
mfs_interface::metadata_op_end(u64 mnum, int cpu, u64 tsc_val)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  mfs_log->update_end_tsc(cpu, tsc_val);
}

// Adds a metadata operation to the logical log.
void
mfs_interface::add_to_metadata_log(u64 mnum, int cpu, mfs_operation *op)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  mfs_log->add_operation(op, cpu);
}

void
mfs_interface::inc_mfslog_linkcount(u64 mnum)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  int cpu = myid();
  scoped_acquire a(&mfs_log->link_lock[cpu]);
  mfs_log->link_count[cpu]++;
}

void
mfs_interface::dec_mfslog_linkcount(u64 mnum)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  int cpu = myid();
  scoped_acquire a(&mfs_log->link_lock[cpu]);
  mfs_log->link_count[cpu]--;
}

u64
mfs_interface::get_mfslog_linkcount(u64 mnum)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));

  u64 count = 0;

  // In order to get a consistent value of the link-count, we need to add up all
  // the per-CPU counts while holding all the per-CPU locks at once. So go on
  // acquiring the locks monotonically, without releasing any in between. This
  // way, adding the counts on-the-go will be equivalent to adding them with all
  // the locks held.
  percpu<scoped_acquire> lk;

  for (int cpu = 0; cpu < NCPU; cpu++) {
    lk[cpu] = mfs_log->link_lock[cpu].guard();
    count += mfs_log->link_count[cpu];
  }

  return count;
}

// Applies all metadata operations logged in the logical logs. Called on sync.
void
mfs_interface::process_metadata_log_and_flush(int cpu)
{
  // Invoke process_metadata_log() on every dirty mnode.
  std::vector<u64> mnum_list;
  metadata_log_htab->enumerate([&](const u64 &mnum, mfs_logical_log* &mfs_log)->bool {

    sref<mnode> m = root_fs->mget(mnum);
    if (m && m->is_dirty()) {
      // In process_metadata_log(), we make decisions based on the mnode's
      // refcount (i.e., whether to free the on-disk inode or postpone it until
      // reboot). So to avoid interference with the refcount, we store the mnode
      // numbers here, and not references to the mnodes themselves (which would
      // have bumped up the refcount inadvertently!).
      mnum_list.push_back(mnum);
    }

      // We call process_metadata_log() outside enumerate() because it does a
      // lookup on metadata_log_htab itself, which causes weird interactions.

    return false;
  });

  for (auto &mnum : mnum_list) {
    sref<mnode> m = root_fs->mget(mnum);
    if (m && m->is_dirty())
      process_metadata_log(get_tsc(), m->mnum_, cpu);
  }

  // Transactions enqueued to the same journal queue (indexed by the cpu number)
  // are always flushed in the order they are enqueued. Hence the transactions
  // generated by process_metadata_log() above go to disk first, followed by
  // those generated by sync_dirty_files_and_dirs().

  sync_dirty_files_and_dirs(cpu, mnum_list);

  {
    // Commit all these transactions via our CPU's per-core journal.
    auto commit_insert_guard = fs_journal[cpu]->commitq_insert_lock.guard();

    for (int i = 0; i < NCPU; i++)  {
      // Delete all the inodes marked for lazy deletion by mnode::onzero()
      std::vector<u64> del_mnum_list;
      {
        auto l = delete_inums[i].lock.guard();
        del_mnum_list = std::move(delete_inums[i].mnum_list);
      }

      for (auto &del_mnum : del_mnum_list) {
        transaction *tr = new transaction();
        delete_mnum_inode_safe(del_mnum, tr, true, true);

	// Note: Queueing dependent transactions on different journals will
	// cause a deadlock here (because we try to flush each journal
	// completely before moving onto the next one). We sidestep this
	// deadlock by committing all these transactions via the same journal
	// (our per-core journal); that's why the second argument has to be
	// 'cpu', as opposed to 'i'.
        add_transaction_to_queue(tr, cpu);
      }
    }
  }

  // Commit and apply pending transactions from ALL the per-core queues, not
  // just the queue we added transactions to above.
  for (int i = 0; i < NCPU; i++)
    flush_transaction_queue(i, true);
}

void
mfs_interface::sync_dirty_files_and_dirs(int cpu, std::vector<u64> &mnum_list)
{
  mfs_logical_log *mfs_log;

  // Invoke sync_file() on every dirty mnode.
  for (auto &mnum : mnum_list) {
    // Some mnodes might linger for a while even after their oplog entries are
    // processed and removed from the metadata-log hash-table. Be careful not to
    // try and process them again!
    if (!metadata_log_htab->lookup(mnum, &mfs_log))
      continue;

    sref<mnode> m = root_fs->mget(mnum);
    if (m && m->is_dirty()) {
      if (m->type() == mnode::types::file)
        m->as_file()->sync_file(cpu);
      else if (m->type() == mnode::types::dir)
        m->as_dir()->sync_dir(cpu);
    }
  }
}

void
mfs_interface::evict_bufcache()
{
  superblock sb;

  cprintf("evict_caches: dropping buffer-cache blocks\n");

  get_superblock(&sb);

  for (u64 inum = 0; inum < sb.ninodes; inum++) {
    u64 mnum;
    sref<mnode> m = mnode_lookup(inum, &mnum);

    if (m && m->type() == mnode::types::file) {
        sref<inode> ip = get_inode(m->mnum_, "evict_bufcache");
        ilock(ip, READLOCK);
        drop_bufcache(ip);
        iunlock(ip);
    }
  }
}

void
mfs_interface::evict_pagecache()
{
  superblock sb;

  cprintf("evict_caches: dropping page-cache pages\n");

  get_superblock(&sb);

  for (u64 inum = 0; inum < sb.ninodes; inum++) {
    u64 mnum;
    sref<mnode> m = mnode_lookup(inum, &mnum);

    if (m && m->type() == mnode::types::file) {
          // Skip uninitialized files, as they won't have any page-cache
          // pages yet. Moreover, file initialization itself consumes
          // some memory (for the radix array), which is undesirable here.
          if (m->is_initialized())
            m->as_file()->drop_pagecache();
    }
  }
}

// Usage:
// To evict the (clean) blocks cached in the buffer-cache, do:
// $ echo 1 > /dev/evict_caches
//
// To evict the (clean) pages cached in the page-cache, do:
// $ echo 2 > /dev/evict_caches
static int
evict_caches(mdev*, const char *buf, u32 n)
{

  if (n != 1) {
    cprintf("evict_caches: invalid number of characters (%d)\n", n);
    return n;
  }

  if (*buf == '1')
    rootfs_interface->evict_bufcache();
  else if (*buf == '2')
    rootfs_interface->evict_pagecache();
  else
    cprintf("evict_caches: invalid option %c\n", *buf);

  return n;
}

void
mfs_interface::apply_rename_pair(std::vector<rename_metadata> &rename_stack,
                                 int cpu)
{
  // The top two operations on the rename stack form a pair.

  rename_metadata rm_1 = rename_stack.back(); rename_stack.pop_back();
  rename_metadata rm_2 = rename_stack.back(); rename_stack.pop_back();

  // Verify that the two rename sub-ops are part of the same higher-level
  // rename operation. Since timestamps are globally unique across all
  // metadata operations, it is sufficient to compare the timestamps.
  assert(rm_1.timestamp == rm_2.timestamp);

  // Lock ordering rule:
  // -------------------
  // Acquire the source directory's mfs_log->lock first, and then the
  // destination directory's mfs_log->lock. These locks are acquired
  // (and held) together only for the duration of the rename operation.

  u64 src_mnum = rm_1.src_parent_mnum;
  u64 dst_mnum = rm_1.dst_parent_mnum;

  mfs_logical_log *mfs_log_src, *mfs_log_dst;
  assert(metadata_log_htab->lookup(src_mnum, &mfs_log_src));
  mfs_log_src->lock.acquire();

  if (dst_mnum != src_mnum) {
    assert(metadata_log_htab->lookup(dst_mnum, &mfs_log_dst));
    mfs_log_dst->lock.acquire();
  }

  std::vector<mfs_operation*> op_vec;

  // Acquire the oplog's sync_lock_ as well, since we will be manipulating
  // the operation vectors as well as their operations.
  {
    auto src_guard = mfs_log_src->synchronize_upto_tsc(rm_1.timestamp);
    auto dst_guard = mfs_log_dst->synchronize_upto_tsc(rm_1.timestamp);

    // After acquiring all the locks, check whether we still have work to do.
    // Note that a concurrent fsync() on the other directory might have
    // flushed out both the rename sub-operations!
    mfs_operation_rename_link *link_op = nullptr;
    mfs_operation_rename_unlink *unlink_op = nullptr;

    if (!mfs_log_src->operation_vec.size() ||
        !mfs_log_dst->operation_vec.size())
      goto unlock;

    link_op =   dynamic_cast<mfs_operation_rename_link*>(
                                    mfs_log_dst->operation_vec.front());
    unlink_op = dynamic_cast<mfs_operation_rename_unlink*>(
                                    mfs_log_src->operation_vec.front());

    if (!(link_op && unlink_op &&
          link_op->timestamp == unlink_op->timestamp &&
          link_op->timestamp == rm_1.timestamp))
      goto unlock;

#if 0
    // Make sure that both parts of the rename operation are applied within
    // the same transaction, to preserve atomicity.
    tr = new transaction(link_op->timestamp);

    // Set 'skip_add' to true, to avoid adding the transaction to the journal's
    // transaction queue before it is fully formed.
    add_op_to_transaction_queue(link_op, cpu, tr, true);
    add_op_to_transaction_queue(unlink_op, cpu, tr);
#else
    op_vec.push_back(link_op);
    op_vec.push_back(unlink_op);
#endif

    // Now we need to delete these two sub-operations from their oplogs.
    // Luckily, we know that as of this moment, both these rename sub-
    // operations are at the beginning of their oplogs (because we have
    // already applied their predecessor operations).
    mfs_log_src->operation_vec.erase(mfs_log_src->operation_vec.begin());
    mfs_log_dst->operation_vec.erase(mfs_log_dst->operation_vec.begin());

  unlock:
    ; // release the locks held by src_guard and dst_guard
  }

  for (auto it = op_vec.begin(); it != op_vec.end(); ) {
    transaction *tr = new transaction((*it)->timestamp);
    add_op_to_transaction_queue(*it, cpu, tr, true);
    it = op_vec.erase(it);
    add_op_to_transaction_queue(*it, cpu, tr);
    it = op_vec.erase(it);
  }

  assert(op_vec.empty());

  if (dst_mnum != src_mnum)
    mfs_log_dst->lock.release();

  mfs_log_src->lock.release();
}

void
mfs_interface::add_op_to_transaction_queue(mfs_operation *op, int cpu,
                                           transaction *tr, bool skip_add)
{
  if (!tr)
    tr = new transaction(op->timestamp);

  op->apply(tr);

  if (!skip_add)
    add_transaction_to_queue(tr, cpu);

  delete op;
}

// Absorbs file link and unlink operations that cancel each other. Absorption
// is aborted if renames are encountered (i.e., operations are not cancelled
// across renames).
//
// TODO: Perform absorption across file renames that don't cross directory
// boundaries.
//
// Called with mfs_log's lock and the oplog's sync_lock_ held.
void
mfs_interface::absorb_file_link_unlink(mfs_logical_log *mfs_log,
                                       std::vector<u64> &absorb_mnum_list)
{
  std::vector<unsigned long> erase_indices;
  u64 htable_size = mfs_log->operation_vec.size() * 5;
  auto linkname_to_index =
                   new chainhash<strbuf<DIRSIZ>, unsigned long>(htable_size);

  for (auto it = mfs_log->operation_vec.begin();
       it != mfs_log->operation_vec.end(); it++) {

    switch ((*it)->operation_type) {

    case MFS_OP_LINK_FILE:
      {
        auto link_op = dynamic_cast<mfs_operation_link*>(*it);
        strbuf<DIRSIZ> name(link_op->name);
        linkname_to_index->insert(name, it - mfs_log->operation_vec.begin());
      }
      break;

    case MFS_OP_UNLINK_FILE:
      {
        unsigned long index;
        auto unlink_op = dynamic_cast<mfs_operation_unlink*>(*it);
        strbuf<DIRSIZ> name(unlink_op->name);
        if (linkname_to_index->lookup(name, &index)) {
          // Mark these link and unlink ops for absorption.
          erase_indices.push_back(it - mfs_log->operation_vec.begin());
          erase_indices.push_back(index);

          dec_mfslog_linkcount(unlink_op->mnode_mnum);
          if (!get_mfslog_linkcount(unlink_op->mnode_mnum)) {

	    // The global link-count of this inode has dropped to zero, which
	    // means that this really is the last unlink of that inode; there
	    // are no other pending links for this inode waiting to be flushed
	    // from other directories/oplogs. So absorb its 'create' operation
	    // if it has not yet been flushed; and delete the inode on the disk
	    // otherwise.
            absorb_mnum_list.push_back(unlink_op->mnode_mnum);
          }
        }
      }
      break;

    case MFS_OP_RENAME_LINK_FILE:
    case MFS_OP_RENAME_LINK_DIR:
    case MFS_OP_RENAME_UNLINK_FILE:
    case MFS_OP_RENAME_UNLINK_DIR:
    case MFS_OP_RENAME_BARRIER:
      // Don't absorb operations across a rename boundary.
      goto out;

    default:
      continue;
    }
  }

out:
  std::sort(erase_indices.begin(), erase_indices.end(),
            std::greater<unsigned long>());

  for (auto &idx : erase_indices) {
    mfs_operation *op = mfs_log->operation_vec.at(idx);
    delete op;
    mfs_log->operation_vec.erase(mfs_log->operation_vec.begin() + idx);
  }

  delete linkname_to_index;
}


// Given an mnode whose last link-unlink pair was absorbed, absorb its 'create'
// operation if it has not yet been flushed, and delete the inode from the disk
// otherwise.
void
mfs_interface::absorb_delete_inode(mfs_logical_log *mfs_log, u64 mnum, int cpu,
                                   std::vector<u64> &unlink_mnum_list)
{

  {
    // Synchronize the oplog loggers.
    auto guard = mfs_log->synchronize_upto_tsc(get_tsc());

    // TODO: Handle directories properly.
    if (!mfs_log->operation_vec.empty() &&
        (mfs_log->operation_vec.front()->operation_type == MFS_OP_CREATE_FILE ||
         mfs_log->operation_vec.front()->operation_type == MFS_OP_CREATE_DIR)) {

      // Simply absorb the 'create' operation.
      mfs_operation *op = mfs_log->operation_vec.front();
      delete op;
      mfs_log->operation_vec.erase(mfs_log->operation_vec.begin());

      // TODO: If this is a directory, its oplog might not be empty. Deal with
      // this case properly.
      assert(mfs_log->operation_vec.empty());

      unlink_mnum_list.push_back(mnum);
      return;
    }
  }

  // Release the spinlock returned by synchronize_upto_tsc() before calling
  // delete_mnum_inode_safe(), because it can sleep.

  // The 'create' operation of this mnode was already flushed to the disk.
  // So we'll have to delete the inode from the disk.
  transaction *tr = new transaction();
  delete_mnum_inode_safe(mnum, tr, true);
  add_transaction_to_queue(tr, cpu);
  unlink_mnum_list.push_back(mnum);
}

// Return values from process_ops_from_oplog():
// -------------------------------------------
enum {
  RET_INVALID = -1,

  // All done (processed operations upto max_tsc in the given mfs_log)
  RET_DONE = 0,

  // Encountered a link operation and added the mnode being linked to the
  // pending stack, as a dependency.
  RET_LINK,

  // Encountered an unlink operation on a directory, and added the directory
  // mnode being unlinked to the pending stack, as a dependency.
  RET_DIRUNLINK,

  // Encountered a rename barrier and added its parent mnode to the pending
  // stack as a dependency.
  RET_RENAME_BARRIER,

  // Encountered a new rename sub-operation and added its counterpart to the
  // pending stack as a dependency.
  RET_RENAME_SUBOP,

  // Got a counterpart for a rename sub-operation, which completes the pair.
  RET_RENAME_PAIR,
};

// process_ops_from_oplog():
//
// Gathers operations from mfs_log with timestamps upto and including 'max_tsc'
// and then processes the first 'count' number of those operations. If count is
// -1, it processes all of them, but if count is 1, it is treated as a special
// case instruction to process only the 'create' operation of the mnode.
// The return values are described above.
int
mfs_interface::process_ops_from_oplog(
                    mfs_logical_log *mfs_log, u64 max_tsc, int count, int cpu,
                    std::vector<pending_metadata> &pending_stack,
                    std::vector<u64> &unlink_mnum_list,
                    std::vector<dirunlink_metadata> &dirunlink_stack,
                    std::vector<rename_metadata> &rename_stack,
                    std::vector<rename_barrier_metadata> &rename_barrier_stack,
                    std::vector<u64> &absorb_mnum_list)
{
  int retval = RET_INVALID;

  struct mfs_ops {
    mfs_operation *op;
    bool op_is_rename;
  };
  std::vector<mfs_ops> op_vec;

  // Synchronize the oplog loggers.
  auto guard = mfs_log->synchronize_upto_tsc(max_tsc);

  if (mfs_log->operation_vec.empty()) {
    retval = RET_DONE;
    goto out;
  }

  // count == 1 is a special case instruction to process only the 'create'
  // operation of the mnode. In all other cases, we process all the operations
  // in the mfs_log (upto and including max_tsc).
  if (count == 1) {
    auto it = mfs_log->operation_vec.begin();
    auto create_op = dynamic_cast<mfs_operation_create*>(*it);
    if (create_op) {
      op_vec.push_back({*it, false});
      mfs_log->operation_vec.erase(it);
    }
    retval = RET_DONE;
    goto out;
  }

  if (mfs_log->operation_vec.size() > 1)
    absorb_file_link_unlink(mfs_log, absorb_mnum_list);

  for (auto it = mfs_log->operation_vec.begin();
       it != mfs_log->operation_vec.end() && (*it)->timestamp <= max_tsc; ) {

    switch ((*it)->operation_type) {

    case MFS_OP_LINK_FILE:
    case MFS_OP_LINK_DIR:
      {
        auto link_op = dynamic_cast<mfs_operation_link*>(*it);
        u64 mnode_inum = 0;
        if (!inum_lookup(link_op->mnode_mnum, &mnode_inum)) {
          // Add the create operation of the mnode being linked as a dependency.
          pending_stack.push_back({link_op->mnode_mnum, link_op->timestamp, 1});
          retval = RET_LINK;
          goto out;
        }
      }
      break;

    case MFS_OP_UNLINK_FILE:
    case MFS_OP_UNLINK_DIR:
      {
        auto unlink_op = dynamic_cast<mfs_operation_unlink*>(*it);
        if (unlink_op->mnode_type == mnode::types::dir) {
          // Flush out all the directory's operations first, before unlinking it.
          auto mnum = unlink_op->mnode_mnum;
          if (dirunlink_stack.size() && mnum == dirunlink_stack.back().mnum) {
            // Already processed.
            dirunlink_stack.pop_back();
            unlink_mnum_list.push_back(mnum);
            // Mark the directory as clean now that it has been flushed.
            sref<mnode> m = root_fs->mget(mnum);
            if (m && m->is_dirty())
              m->dirty(false);
            op_vec.push_back({*it, false});
            it = mfs_log->operation_vec.erase(it);
            continue;
          }

          dirunlink_stack.push_back({mnum});
          pending_stack.push_back({mnum, get_tsc(), -1});
          retval = RET_DIRUNLINK;
          goto out;
        } else {
          unlink_mnum_list.push_back(unlink_op->mnode_mnum);
        }
      }
      break;

    case MFS_OP_RENAME_BARRIER:
      {
        auto rename_barrier_op = dynamic_cast<mfs_operation_rename_barrier*>(*it);
        if (rename_barrier_op->mnode_mnum == root_mnum) {
          // Nothing to be done.
          it = mfs_log->operation_vec.erase(it);

          // Retry absorption after processing a rename, if we are not exiting
          // this function.
          if (mfs_log->operation_vec.size() > 1) {
            absorb_file_link_unlink(mfs_log, absorb_mnum_list);
            it = mfs_log->operation_vec.begin();
          }
          continue;
        }

        auto mnum = rename_barrier_op->mnode_mnum;
        auto parent_mnum = rename_barrier_op->parent_mnum;
        auto timestamp = rename_barrier_op->timestamp;

        if (rename_barrier_stack.size() &&
            mnum == rename_barrier_stack.back().mnode_mnum &&
            timestamp == rename_barrier_stack.back().timestamp) {
          // Already processed.
          rename_barrier_stack.pop_back();
          it = mfs_log->operation_vec.erase(it);

          // Retry absorption after processing a rename, if we are not exiting
          // this function.
          if (mfs_log->operation_vec.size() > 1) {
            absorb_file_link_unlink(mfs_log, absorb_mnum_list);
            it = mfs_log->operation_vec.begin();
          }
          continue;
        }

        rename_barrier_stack.push_back({mnum, timestamp});
        pending_stack.push_back({parent_mnum, timestamp, -1});
        retval = RET_RENAME_BARRIER;
        goto out;
      }
      break;

    case MFS_OP_RENAME_LINK_FILE:
    case MFS_OP_RENAME_LINK_DIR:
    case MFS_OP_RENAME_UNLINK_FILE:
    case MFS_OP_RENAME_UNLINK_DIR:
      {
        auto rename_link_op = dynamic_cast<mfs_operation_rename_link*>(*it);
        auto rename_unlink_op = dynamic_cast<mfs_operation_rename_unlink*>(*it);

        // If this not a cross-directory rename, deal with it separately. If
        // that's the case indeed, we are guaranteed to find rename-link-op first,
        // followed by rename-unlink-op.
        // TODO: Modify apply_rename_pair() to also handle this, instead of treating
        // it as a special case here.
        if (rename_link_op &&
            rename_link_op->src_parent_mnum == rename_link_op->dst_parent_mnum) {

#if 0
          transaction *tr = nullptr;

          // Make sure that both parts of the rename operation are applied within
          // the same transaction, to preserve atomicity.
          tr = new transaction(rename_link_op->timestamp);

          // Set 'skip_add' to true, to avoid adding the transaction to the journal
          // before it is fully formed.
          add_op_to_transaction_queue(rename_link_op, cpu, tr, true);
#else
          op_vec.push_back({rename_link_op, true});
#endif
          it = mfs_log->operation_vec.erase(it);

          // The very next operation in this oplog *has* to be the corresponding
          // rename_unlink_op.
          auto r_unlink_op = dynamic_cast<mfs_operation_rename_unlink*>(*it);
          assert(r_unlink_op && r_unlink_op->timestamp == rename_link_op->timestamp
                 && r_unlink_op->src_parent_mnum == r_unlink_op->dst_parent_mnum);

#if 0
          add_op_to_transaction_queue(r_unlink_op, cpu, tr);
#else
          op_vec.push_back({r_unlink_op, true});
#endif
          it = mfs_log->operation_vec.erase(it);

          // Retry absorption after processing a rename, if we are not exiting
          // this function.
          if (mfs_log->operation_vec.size() > 1) {
            absorb_file_link_unlink(mfs_log, absorb_mnum_list);
            it = mfs_log->operation_vec.begin();
          }
          continue;
        } else if (rename_unlink_op &&
                   rename_unlink_op->src_parent_mnum ==
                   rename_unlink_op->dst_parent_mnum) {

          // The very next operation in this oplog *has* to be the corresponding
          // rename_link_op.
          auto r_link_op = dynamic_cast<mfs_operation_rename_link*>(*(it+1));
          assert(r_link_op && r_link_op->timestamp == rename_unlink_op->timestamp
                 && r_link_op->src_parent_mnum == r_link_op->dst_parent_mnum);

#if 0
          transaction *tr = nullptr;

          // Make sure that both parts of the rename operation are applied within
          // the same transaction, to preserve atomicity.
          tr = new transaction(rename_unlink_op->timestamp);

          // Set 'skip_add' to true, to avoid adding the transaction to the journal
          // before it is fully formed.
          add_op_to_transaction_queue(r_link_op, cpu, tr, true);
          add_op_to_transaction_queue(rename_unlink_op, cpu, tr);
#else
          op_vec.push_back({r_link_op, true});
          op_vec.push_back({rename_unlink_op, true});
#endif

          it = mfs_log->operation_vec.erase(it);
          it = mfs_log->operation_vec.erase(it);

          // Retry absorption after processing a rename, if we are not exiting
          // this function.
          if (mfs_log->operation_vec.size() > 1) {
            absorb_file_link_unlink(mfs_log, absorb_mnum_list);
            it = mfs_log->operation_vec.begin();
          }
          continue;
        }

        if (rename_link_op)
          assert(rename_link_op->src_parent_mnum != rename_link_op->dst_parent_mnum);

        if (rename_unlink_op)
          assert(rename_unlink_op->src_parent_mnum != rename_unlink_op->dst_parent_mnum);

        // Cross-directory renames, of both files and directories are handled below.

        // Check if this is the counterpart of the latest rename sub-operation
        // that we know of.

        u64 rename_timestamp = 0;
        if (rename_stack.size())
          rename_timestamp = rename_stack.back().timestamp;

        if (rename_link_op) {
          rename_stack.push_back({rename_link_op->src_parent_mnum,
                                  rename_link_op->dst_parent_mnum,
                                  rename_link_op->timestamp});
          // We have the link part of the rename, so add the unlink part as a
          // dependency, if we haven't done so already.
          if (!(rename_timestamp && (*it)->timestamp == rename_timestamp))
            pending_stack.push_back({rename_link_op->src_parent_mnum,
                                     rename_link_op->timestamp, -1});
        } else if (rename_unlink_op) {
          rename_stack.push_back({rename_unlink_op->src_parent_mnum,
                                  rename_unlink_op->dst_parent_mnum,
                                  rename_unlink_op->timestamp});
          // We have the unlink part of the rename, so add the link part as a
          // dependency, if we haven't done so already.
          if (!(rename_timestamp && (*it)->timestamp == rename_timestamp))
            pending_stack.push_back({rename_unlink_op->dst_parent_mnum,
                                     rename_unlink_op->timestamp, -1});
        }

        if (rename_timestamp && (*it)->timestamp == rename_timestamp) {
          retval = RET_RENAME_PAIR;
          goto out;
        }
        retval = RET_RENAME_SUBOP;
        goto out;
      }
      break;

    }

    op_vec.push_back({*it, false});
    it = mfs_log->operation_vec.erase(it);
  }

  assert(mfs_log->operation_vec.empty() ||
         mfs_log->operation_vec.front()->timestamp > max_tsc);

out:
  if (retval == RET_INVALID)
    retval = RET_DONE;

  if (op_vec.empty())
    return retval;

  // Release the sync-lock (which is a spinlock) because
  // add_op_to_transaction_queue() can sleep.
  guard.release();

  for (auto it = op_vec.begin(); it != op_vec.end(); ) {
    if ((*it).op_is_rename) {
      // Make sure that both parts of the rename operation are applied within
      // the same transaction, to preserve atomicity.
      transaction *tr = new transaction((*it).op->timestamp);

      // Set 'skip_add' to true, to avoid adding the transaction to the journal
      // before it is fully formed.
      add_op_to_transaction_queue((*it).op, cpu, tr, true);
      it = op_vec.erase(it);

      assert((*it).op_is_rename);
      add_op_to_transaction_queue((*it).op, cpu, tr);
      it = op_vec.erase(it);
    } else {
      add_op_to_transaction_queue((*it).op, cpu);
      it = op_vec.erase(it);
    }
  }
  assert(op_vec.empty());
  return retval;
}

// Applies metadata operations logged in the logical journal. Called on
// fsync to resolve any metadata dependencies.
void
mfs_interface::process_metadata_log(u64 max_tsc, u64 mnode_mnum, int cpu)
{
  std::vector<pending_metadata> pending_stack;
  std::vector<u64> unlink_mnum_list;
  std::vector<dirunlink_metadata> dirunlink_stack;
  std::vector<rename_metadata> rename_stack;
  std::vector<rename_barrier_metadata> rename_barrier_stack;
  std::vector<u64> absorb_mnum_list;
  int ret;

  auto commit_insert_guard = fs_journal[cpu]->commitq_insert_lock.guard();

  // Delete all the inodes marked for lazy deletion by mnode::onzero()
  {
    std::vector<u64> del_mnum_list;
    {
      auto l = delete_inums[cpu].lock.guard();
      del_mnum_list = std::move(delete_inums[cpu].mnum_list);
    }
    for (auto &del_mnum : del_mnum_list) {
      transaction *tr = new transaction();
      delete_mnum_inode_safe(del_mnum, tr, true, true);
      add_transaction_to_queue(tr, cpu);
    }
  }

  pending_stack.push_back({mnode_mnum, max_tsc, -1});

  while (pending_stack.size()) {
    pending_metadata pm = pending_stack.back();

    mfs_logical_log *mfs_log = nullptr;
    if (!metadata_log_htab->lookup(pm.mnum, &mfs_log)) {
      pending_stack.pop_back();
      continue;
    }

    mfs_log->lock.acquire();
    ret = process_ops_from_oplog(mfs_log, pm.max_tsc, pm.count, cpu, pending_stack,
                                 unlink_mnum_list, dirunlink_stack, rename_stack,
                                 rename_barrier_stack, absorb_mnum_list);
    mfs_log->lock.release();

    switch (ret) {

    case RET_DONE:
      // Handling renames is a 2-step process involving a RET_RENAME_SUBOP,
      // followed by a RET_RENAME_PAIR. However, with concurrent fsync()s, we
      // can end up getting a RET_DONE when we were expecting a RET_RENAME_PAIR,
      // if another fsync() call processes that rename operation faster. Fix up
      // the rename_stack here in those situations.
      {
        rename_metadata rm = rename_stack.back();
        if (pm.max_tsc == rm.timestamp &&
             (pm.mnum == rm.src_parent_mnum || pm.mnum == rm.dst_parent_mnum)) {
          rename_stack.pop_back();
        }
      }
      pending_stack.pop_back();
      break;

    case RET_LINK:
    case RET_DIRUNLINK:
    case RET_RENAME_BARRIER:
    case RET_RENAME_SUBOP:
      continue;

    // Now that we got the complete rename pair, acquire the necessary locks
    // and apply both parts of the rename atomically using a single transaction.
    case RET_RENAME_PAIR:
      apply_rename_pair(rename_stack, cpu);
      // Since the rename sub-operations got paired up and were applied, we
      // don't have to process the other directory any further for this fsync
      // call. So pop it off the pending stack.
      pending_stack.pop_back();
      break;

    default:
      panic("Got invalid return code from process_ops_from_oplog()");
    }
  }

  assert(pending_stack.empty());
  assert(dirunlink_stack.empty());
  assert(rename_barrier_stack.empty());
  assert(rename_stack.empty());

  // absorb_mnum_list contains the list of mnodes whose last link-unlink pair
  // was absorbed (which means that their on-disk link count is 0 and there are
  // no more links to those inodes waiting to be flushed from other directories
  // or oplogs). So if the 'create' operation of those mnodes haven't been
  // flushed yet, absorb them too. Otherwise, delete those inodes from the disk.
  for (auto &mnum : absorb_mnum_list) {
    mfs_logical_log *mfs_log;
    assert(metadata_log_htab->lookup(mnum, &mfs_log));

    mfs_log->lock.acquire();
    absorb_delete_inode(mfs_log, mnum, cpu, unlink_mnum_list);
    mfs_log->lock.release();
  }

  // Release the auxiliary resources of recently deleted mnodes, now that we
  // are sure that we won't need them any more.
  for (auto &mnum : unlink_mnum_list) {
    u64 inum;
    if (!inum_lookup(mnum, &inum)) {
      // delete_mnum_inode() removes the mnum from the mnum_to_inum hash-table.
      // So failing this lookup is a reliable indication (in this particular
      // context) that this mnode was deleted already.
      free_metadata_log(mnum);
      free_mnode_lock(mnum);
    }
  }

  // If we just processed a directory, mark it clean, since that's all there is
  // to flushing a directory.
  sref<mnode> m = root_fs->mget(mnode_mnum);
  if (m && m->type() == mnode::types::dir && m->is_dirty())
    m->dirty(false);
}

// Create operation
void
mfs_interface::mfs_create(mfs_operation_create *op, transaction *tr)
{
  scoped_gc_epoch e;

  if (op->mnode_type == mnode::types::file)
    create_file(op->mnode_mnum, op->mnode_type, tr);
  else if (op->mnode_type == mnode::types::dir)
    create_dir(op->mnode_mnum, op->parent_mnum, op->mnode_type, tr);
}

// Link operation
void
mfs_interface::mfs_link(mfs_operation_link *op, transaction *tr)
{
  scoped_gc_epoch e;
  add_dir_entry(op->parent_mnum, op->name, op->mnode_mnum,
                         op->mnode_type, tr);
}

// Unlink operation
void
mfs_interface::mfs_unlink(mfs_operation_unlink *op, transaction *tr)
{
  scoped_gc_epoch e;
  remove_dir_entry(op->parent_mnum, op->name, tr);
}

// Rename Link operation
void
mfs_interface::mfs_rename_link(mfs_operation_rename_link *op, transaction *tr)
{
  scoped_gc_epoch e;

  u64 mnode_inum, src_parent_inum, dst_parent_inum;
  assert(inum_lookup(op->mnode_mnum, &mnode_inum));
  assert(inum_lookup(op->src_parent_mnum, &src_parent_inum));
  assert(inum_lookup(op->dst_parent_mnum, &dst_parent_inum));

  // Lock ordering rule: Acquire all inode-block locks before performing any
  // ilock().
  std::vector<u64> inum_list;
  inum_list.push_back(mnode_inum);
  inum_list.push_back(src_parent_inum);
  if (dst_parent_inum != src_parent_inum)
    inum_list.push_back(dst_parent_inum);

  acquire_inodebitmap_locks(inum_list, INODE_BLOCK, tr);

  // Buffer-cache updates start here.
  add_dir_entry(op->dst_parent_mnum, op->newname, op->mnode_mnum,
                op->mnode_type, tr, true);

  if (op->mnode_type == mnode::types::dir &&
      op->dst_parent_mnum != op->src_parent_mnum) {

    sref<inode> ip = iget(1, mnode_inum);

    // No need to acquire write-lock on src-parent or dst-parent because calls
    // to dirlink/dirunlink which only alter the ".." links don't modify the
    // parent directories in any way.
    ilock(ip, WRITELOCK);
    dirunlink(ip, "..", src_parent_inum, false, tr);
    dirlink(ip, "..", dst_parent_inum, false, tr);
    iunlock(ip);
  }
}

// Rename Unlink operation
void
mfs_interface::mfs_rename_unlink(mfs_operation_rename_unlink *op, transaction *tr)
{
  scoped_gc_epoch e;

  // Buffer-cache updates start in mfs_rename_link() itself.
  remove_dir_entry(op->src_parent_mnum, op->name, tr, true);
}

void
mfs_interface::add_transaction_to_queue(transaction *tr, int cpu)
{
  pre_process_transaction(tr);

  // As of this moment, we hold all the locks we need: all the 2-Phase inode-
  // block and bitmap-block locks and the lock protecting the transaction
  // queue for this journal.
  tr->enq_tsc = get_tsc();
  tr->last_group_txn_tsc = tr->enq_tsc;
  tr->txq_id = cpu;

  tx_queue_info my_txq(tr->txq_id, tr->enq_tsc);

  for (auto &blknum : tr->inodebitmap_blk_list) {
    tx_queue_info other_txq;

    // Note down the last transaction that modified a common disk block,
    // if it got added to a different queue. (If it went to the same queue
    // that we are going to, the ordering will be automatically preserved).
    if (blocknum_to_queue->lookup(blknum, &other_txq)) {
      if (other_txq.id_ != tr->txq_id)
        tr->dependent_txq.push_back(other_txq);

      blocknum_to_queue->remove(blknum);
    }

    // The insert has to succeed because we are holding all the relevant
    // 2-Phase locks; so no other CPU can be inserting to the same blocknum
    // concurrently.
    assert(blocknum_to_queue->insert(blknum, my_txq));
  }

  // Phase 2 of the 2-Phase locking. Updating the blocknum_to_queue hash-table
  // with this transaction's cpu and timestamp is sufficient to help us preserve
  // the ordering between dependent transactions across different cores. So it
  // is safe to execute phase 2 and release the locks here.
  release_inodebitmap_locks(tr);

  {
    auto cq_guard = fs_journal[cpu]->tx_commit_queue_lock.guard();
    fs_journal[cpu]->enqueue_transaction(tr);

    // Once the tx_commit_queue_lock is released, the transaction that we just
    // enqueued can be dequeued by the commit-side code and can even be deleted
    // after processing. So we should not make any more updates to this
    // transaction after this point.
  }
}

void
mfs_interface::pre_process_transaction(transaction *tr)
{
  std::sort(tr->allocated_block_list.begin(), tr->allocated_block_list.end());
  std::sort(tr->free_block_list.begin(), tr->free_block_list.end());

  std::vector<u64> bnum_list;
  for (auto &b : tr->allocated_block_list)
    bnum_list.push_back(b);
  for (auto &b : tr->free_block_list)
    bnum_list.push_back(b);

  // End of Phase 1 of the 2-Phase locking.
  acquire_inodebitmap_locks(bnum_list, BITMAP_BLOCK, tr);

  // Update the free bitmap on the disk.
  if (!tr->allocated_block_list.empty())
    balloc_on_disk(tr->allocated_block_list, tr);

  if (!tr->free_block_list.empty())
    bfree_on_disk(tr->free_block_list, tr);
}

void
mfs_interface::post_process_transaction(transaction *tr)
{
  // Now that the transaction has been committed, mark the freed blocks as
  // free in the in-memory free-bit-vector.
  for (auto &f : tr->free_block_list)
    free_block(f);

  // Make the freed inode numbers available again for reuse.
  for (auto &inum : tr->free_inum_list)
    free_inode_number(inum);
}

void
mfs_interface::apply_trans_on_disk(transaction *tr)
{
  // This transaction has been committed to the journal. Writeback the changes
  // to the original locations on the disk.
  tr->write_to_disk_and_flush();
}

#if 0
void
mfs_interface::commit_transactions(std::vector<transaction*> &tx_queue,
                                   transaction *dedup_trans, int cpu)
{
  assert(!tx_queue.empty());

  u64 timestamp = tx_queue.front()->enq_tsc;
  transaction *jrnl_trans = new transaction();

  ilock(sv6_journal[cpu], WRITELOCK);

  // Write the transaction's start block and the data blocks to the on-disk
  // journal.
  write_journal_transaction_blocks(dedup_trans->blocks, timestamp, jrnl_trans,
                                   cpu);
  delete jrnl_trans;

  // Postpone committing this batch of transactions until all the dependent
  // transactions in other queues have been committed to the disk. It is
  // sufficient to look at the first transaction in the batch, since that's
  // the only transaction allowed to have any cross-queue dependencies.
  for (auto dep_txn : tx_queue.front()->dependent_txq) {
    while (fs_journal[dep_txn.id_]->get_committed_tsc() < dep_txn.timestamp_)
      fs_journal[dep_txn.id_]->wait_for_commit(dep_txn.timestamp_);
  }

  // Commit the transaction to the on-disk journal with the given timestamp.
  write_journal_commit_block(timestamp, cpu);
  iunlock(sv6_journal[cpu]);

  // Notify transactions (in other journal queues) which were waiting for
  // this particular batch of transactions to get committed.
  u64 latest_commit_tsc = tx_queue.back()->enq_tsc;
  fs_journal[cpu]->notify_commit(latest_commit_tsc);

  for (auto &tr : tx_queue)
    post_process_transaction(tr);
}

void
mfs_interface::apply_transactions(std::vector<transaction*> &tx_queue,
                                  transaction *dedup_trans, int cpu)
{
  assert(!tx_queue.empty());

  // Postpone applying this batch of transactions until all the dependent
  // transactions in other queues have been applied to the disk. It is
  // sufficient to look at the first transaction in the batch, since that's
  // the only transaction allowed to have any cross-queue dependencies.
  for (auto dep_txn : tx_queue.front()->dependent_txq) {
    while (fs_journal[dep_txn.id_]->get_applied_tsc() < dep_txn.timestamp_) {

      // The other CPU's transactions might not get applied if there is still
      // sufficient space in its journal and is hence continuing to batch
      // transactions for a bulk-apply. So force-apply the other CPU's
      // transactions to disk to avoid waiting on them forever. Also, since we
      // cannot guarantee a consistent ordering between acquiring the
      // per-journal locks, use try-acquire to avoid deadlocks.
      auto guard = fs_journal[dep_txn.id_]->lock.try_guard();

      if (!guard)
        continue;

      // Force-apply dependent transactions from the other CPU's journal.
      flush_journal_locked(dep_txn.id_, true);
      fs_journal[dep_txn.id_]->wait_for_apply(dep_txn.timestamp_);
    }
  }

  // Apply all the committed sub-transactions to their final destinations
  // on the disk.
  apply_trans_on_disk(dedup_trans);

  for (auto &tr : tx_queue)
    tr->dependent_txq.clear();

  // Notify transactions (in other journal queues) which were waiting for
  // this particular batch of transactions to get applied to the on-disk
  // filesystem.
  u64 latest_apply_tsc = tx_queue.back()->enq_tsc;
  fs_journal[cpu]->notify_apply(latest_apply_tsc);

  for (auto &tr : tx_queue)
    delete tr;
}

// Writes out the physical journal to the disk, and applies the committed
// transactions to the disk filesystem.
void
mfs_interface::flush_journal_locked(int cpu, bool apply_all)
{
  bool apply_and_flush = apply_all;
  std::vector<transaction*> local_commit_queue;
  transaction *dedup_trans;

  while (!fs_journal[cpu]->tx_commit_queue.empty() ||
         (apply_all && !fs_journal[cpu]->tx_apply_queue.empty())) {

    dedup_trans = new transaction();

    for (auto it = fs_journal[cpu]->tx_commit_queue.begin();
         it != fs_journal[cpu]->tx_commit_queue.end(); ) {

      (*it)->deduplicate_blocks();

      // To avoid deadlocks, we allow only the head transaction in any batch to
      // have cross-queue (i.e., cross-journal) dependencies. If we encounter a
      // later transaction with a cross-queue dependency, we close the batching
      // window first, flush the existing batch of transactions and then create
      // a new batch for the other transaction and its successors. See the
      // commit's changelog for details about deadlocks.
      if (((*it)->dependent_txq.empty() || local_commit_queue.empty())
          && fits_in_journal(dedup_trans->blocks.size() + (*it)->blocks.size(),
                             cpu)) {

        local_commit_queue.push_back(*it);
        dedup_trans->add_blocks(std::move((*it)->blocks));
        dedup_trans->deduplicate_blocks();

        it = fs_journal[cpu]->tx_commit_queue.erase(it);

      } else {
        // No space left in the journal to accommodate this sub-transaction.
        // So commit and apply all the earlier sub-transactions, to make space
        // for the remaining sub-transactions.
        apply_and_flush = true;
        break;
      }
    }

    if (!local_commit_queue.empty())
      commit_transactions(local_commit_queue, dedup_trans, cpu);

    // Save the committed transactions to the apply-queue, so that we can
    // postpone applying them if we wish.
    for (auto &tr : local_commit_queue)
      fs_journal[cpu]->tx_apply_queue.push_back(tr);

    fs_journal[cpu]->apply_dedup_trans->add_blocks(std::move(dedup_trans->blocks));
    fs_journal[cpu]->apply_dedup_trans->deduplicate_blocks();
    delete dedup_trans;

    local_commit_queue.clear();

    if (!apply_and_flush)
      continue;

    apply_transactions(fs_journal[cpu]->tx_apply_queue,
                       fs_journal[cpu]->apply_dedup_trans, cpu);

    delete fs_journal[cpu]->apply_dedup_trans;
    fs_journal[cpu]->apply_dedup_trans = new transaction();

    fs_journal[cpu]->tx_apply_queue.clear();

    ilock(sv6_journal[cpu], WRITELOCK);
    reset_journal(cpu);
    iunlock(sv6_journal[cpu]);

    // Reset the apply knob, to avoid applying needlessly in subsequent
    // iterations.
    apply_and_flush = apply_all;
  }
}

void
mfs_interface::flush_journal(int cpu, bool apply_all)
{
  auto journal_lock = fs_journal[cpu]->lock.guard();
  flush_journal_locked(cpu, apply_all);
}
#endif

void
mfs_interface::commit_transaction_to_disk(int cpu, transaction *trans)
{
  u64 timestamp = trans->commit_tsc;
  transaction *jrnl_trans = new transaction();

  ilock(sv6_journal[cpu], WRITELOCK);

  // Write the transaction's start block and the data blocks to the on-disk
  // journal.
  write_journal_transaction_blocks(trans->blocks, timestamp, jrnl_trans,
                                   cpu);
  iunlock(sv6_journal[cpu]);
  delete jrnl_trans;

  // Commit the transaction to the on-disk journal with the given timestamp.
  ilock(sv6_journal[cpu], WRITELOCK);
  write_journal_commit_block(timestamp, cpu);
  iunlock(sv6_journal[cpu]);

  post_process_transaction(trans);

  // Notify transactions (in other journal queues) which were waiting for
  // this particular batch of transactions to get committed.
  u64 latest_commit_tsc = trans->last_group_txn_tsc;
  fs_journal[cpu]->notify_commit(latest_commit_tsc);
}

void
mfs_interface::apply_transaction_to_disk(int cpu, transaction *trans)
{
  // Apply all the committed sub-transactions to their final destinations
  // on the disk.
  apply_trans_on_disk(trans);

  // Notify transactions (in other journal queues) which were waiting for
  // this particular batch of transactions to get applied to the on-disk
  // filesystem.
  u64 latest_apply_tsc = trans->last_group_txn_tsc;
  fs_journal[cpu]->notify_apply(latest_apply_tsc);

  delete trans;
}

void
mfs_interface::commit_all_transactions(int cpu)
{
  for (;;) {
    u64 enq_tsc = 0;
    u64 blocks_size = 0;
    std::vector<tx_queue_info> dependent_txq;

    {
      auto commit_remove_guard = fs_journal[cpu]->commitq_remove_lock.guard();

      // We hold the commitq_remove_lock above (even though we are not actually
      // removing transactions from the queue yet), so that we don't observe an
      // empty commit-queue and return early while the (other) thread that
      // dequeued the last transaction is still committing it to the disk.
      // However, this is only to make things look symmetric with the apply-code
      // and avoid any nasty surprises; in practice though, we hold the per-core
      // journal's journal_lock before invoking this function, so there can only
      // be one thread committing transactions to a given per-core journal at a
      // time (unlike apply).

      auto cq_guard = fs_journal[cpu]->tx_commit_queue_lock.guard();

      if (fs_journal[cpu]->tx_commit_queue.empty())
        return;

      transaction *tr = fs_journal[cpu]->tx_commit_queue.front();
      enq_tsc = tr->enq_tsc;
      for (auto &dep_txn : tr->dependent_txq)
        dependent_txq.push_back(dep_txn);

      tr->deduplicate_blocks();
      blocks_size = tr->blocks.size();
    }

    if (!fits_in_journal(blocks_size, cpu)) {
      apply_all_transactions(cpu);
      assert(fits_in_journal(blocks_size, cpu));
    }

    // Postpone committing this batch of transactions until all the dependent
    // transactions in other queues have been committed to the disk. It is
    // sufficient to look at the first transaction in the batch, since that's
    // the only transaction allowed to have any cross-queue dependencies.
    for (auto &dep_txn : dependent_txq) {
      while (fs_journal[dep_txn.id_]->get_committed_tsc() < dep_txn.timestamp_)
        fs_journal[dep_txn.id_]->wait_for_commit(dep_txn.timestamp_);
    }

    transaction *trans = nullptr;
    auto commit_remove_guard = fs_journal[cpu]->commitq_remove_lock.guard();
    {
      auto cq_guard = fs_journal[cpu]->tx_commit_queue_lock.guard();

      // The commit-queue should not have shrunk in the meantime, because we
      // hold this per-cpu journal's journal_lock (acquired by our caller).
      assert(!fs_journal[cpu]->tx_commit_queue.empty());
      assert(fs_journal[cpu]->tx_commit_queue.front()->enq_tsc == enq_tsc);

      auto it = fs_journal[cpu]->tx_commit_queue.begin();
      trans = *it;
      it = fs_journal[cpu]->tx_commit_queue.erase(it);

      for ( ; it != fs_journal[cpu]->tx_commit_queue.end(); ) {
        if ((*it)->dependent_txq.empty() == false)
          break;

        // This transaction doesn't have cross-queue dependencies, so try to
        // merge it with the other transaction and commit them together.
        (*it)->deduplicate_blocks();
        if (!fits_in_journal(trans->blocks.size() + (*it)->blocks.size(), cpu))
          break;

        trans->add_blocks(std::move((*it)->blocks));
        trans->deduplicate_blocks();
        assert(fits_in_journal(trans->blocks.size(), cpu));

        trans->last_group_txn_tsc = (*it)->enq_tsc;
        assert(trans->last_group_txn_tsc > trans->enq_tsc);

        delete *it;
        it = fs_journal[cpu]->tx_commit_queue.erase(it);
      }
    }

    trans->commit_tsc = get_tsc();

    commit_transaction_to_disk(cpu, trans);

    // Move the committed transaction to the apply queue.
    auto apply_insert_guard = fs_journal[cpu]->applyq_insert_lock.guard();
    {
      auto aq_guard = fs_journal[cpu]->tx_apply_queue_lock.guard();
      fs_journal[cpu]->tx_apply_queue.push_back(trans);
    }
  }
}

void
mfs_interface::apply_all_transactions(int cpu)
{
  std::vector<tx_queue_info> dependent_txq;
  {
    auto apply_remove_guard = fs_journal[cpu]->applyq_remove_lock.guard();

    // We need to hold the applyq_remove_lock above (even though we are not
    // actually removing transactions from the queue yet), so as to maintain
    // this invariant: if the apply-queue is empty, the journal's offset should
    // be zero.
    // We might observe an empty apply-queue here due to a concurrent thread
    // dequeuing and applying the last transaction in that queue; in that case,
    // we need to wait for the journal to be cleared by that thread, before
    // returning. Since the dequeue-apply-clear-journal sequence is performed
    // with the applyq_remove_lock held, we just need to observe the state of
    // the apply-queue with the same lock held.

    auto apply_guard = fs_journal[cpu]->tx_apply_queue_lock.guard();
    if (fs_journal[cpu]->tx_apply_queue.empty())
      return;

    dependent_txq.push_back({cpu,
                            fs_journal[cpu]->tx_apply_queue.back()->enq_tsc});
  }

  while (dependent_txq.size()) {
    transaction *tr = nullptr;
    tx_queue_info txq = dependent_txq.back();

    int dep_cpu = txq.id_;
    u64 dep_tsc = txq.timestamp_;

    if (fs_journal[dep_cpu]->get_applied_tsc() >= dep_tsc) {
      dependent_txq.pop_back();
      continue;
    }

    auto apply_remove_guard = fs_journal[dep_cpu]->applyq_remove_lock.guard();
    {
      auto apply_guard = fs_journal[dep_cpu]->tx_apply_queue_lock.guard();

      // applied_trans_tsc is updated with the applyq_remove_lock held. So this
      // check will be accurate.
      if (fs_journal[dep_cpu]->get_applied_tsc() >= dep_tsc) {
        dependent_txq.pop_back();
        continue;
      }

      // applied_trans_tsc is still less than what we want, and the apply queue
      // is empty. That means the transaction has been committed and is about to
      // be moved to the apply queue. So give that process a chance to acquire
      // the tx_apply_queue_lock in order to finish the move, and then try again
      // to apply that transaction.
      if (fs_journal[dep_cpu]->tx_apply_queue.empty())
        continue;

      auto it = fs_journal[dep_cpu]->tx_apply_queue.begin();
      tr = *it;

      assert(tr->enq_tsc <= dep_tsc);

      u64 txq_size = dependent_txq.size();
      for (auto &dep_txn : tr->dependent_txq) {
        if (fs_journal[dep_txn.id_]->get_applied_tsc() < dep_txn.timestamp_)
          dependent_txq.push_back(dep_txn);
      }

      // If we added any new nested dependencies, process them first.
      if (dependent_txq.size() != txq_size) {
        assert(dependent_txq.size() > txq_size);
        continue;
      }

      fs_journal[dep_cpu]->tx_apply_queue.erase(it);
    }

    apply_transaction_to_disk(dep_cpu, tr);

    if (fs_journal[dep_cpu]->get_applied_tsc() >= dep_tsc)
      dependent_txq.pop_back();

    // Clear the journal if we emptied the transaction-apply queue.
    {
      auto apply_guard = fs_journal[dep_cpu]->tx_apply_queue_lock.guard();
      if (!fs_journal[dep_cpu]->tx_apply_queue.empty())
        continue;
    }

    ilock(sv6_journal[dep_cpu], WRITELOCK);
    reset_journal(dep_cpu);
    iunlock(sv6_journal[dep_cpu]);
  }
}

void
mfs_interface::flush_transaction_queue(int cpu, bool apply_transactions)
{
  auto journal_guard = fs_journal[cpu]->journal_lock.guard();

  commit_all_transactions(cpu);

  // Apply all the committed transactions from the per-core journal to the
  // filesystem, if explicitly requested by the caller.
  if (apply_transactions)
    apply_all_transactions(cpu);
}

void
mfs_interface::print_txq_stats()
{
  // We intentionally avoid taking any locks here, because this function is
  // typically invoked by the user when the system has already deadlocked;
  // we don't want to make it any worse.

  cprintf("TRANSACTION COMMIT QUEUES:\n");
  for (int cpu = 0; cpu < NCPU; cpu++) {
    if (fs_journal[cpu]->tx_commit_queue.empty())
      continue;

    cprintf("CPU %d: committed_upto: %lu\n", cpu, fs_journal[cpu]->get_committed_tsc());
    for (auto &t : fs_journal[cpu]->tx_commit_queue) {
      cprintf("cpu %d txn %lu depends on \n", cpu, t->enq_tsc);
      for (auto &d : t->dependent_txq) {
        cprintf("    dcpu %d dtxn %lu\n", d.id_, d.timestamp_);
      }
    }
  }

  cprintf("TRANSACTION APPLY QUEUES:\n");
  for (int cpu = 0; cpu < NCPU; cpu++) {
    if (fs_journal[cpu]->tx_apply_queue.empty())
      continue;

    cprintf("CPU %d: applied_upto: %lu\n\n", cpu, fs_journal[cpu]->get_applied_tsc());
    for (auto &t : fs_journal[cpu]->tx_apply_queue) {
      cprintf("cpu %d txn %lu depends on \n", cpu, t->enq_tsc);
      for (auto &d : t->dependent_txq) {
        cprintf("    dcpu %d dtxn %lu\n", d.id_, d.timestamp_);
      }
    }
  }

  cprintf("COMMIT DEPENDENCIES:\n");
  for (int cpu = 0; cpu < NCPU; cpu++) {
    if (fs_journal[cpu]->tx_commit_queue.empty())
      continue;

    auto &t = fs_journal[cpu]->tx_commit_queue.front();
    if (t->dependent_txq.empty())
      continue;
    for (auto &d : t->dependent_txq) {
      if (fs_journal[d.id_]->get_committed_tsc() < d.timestamp_)
        cprintf("cpu %d waits for commit on dcpu %d\n", cpu, d.id_);
    }
  }

  cprintf("APPLY DEPENDENCIES:\n");
  for (int cpu = 0; cpu < NCPU; cpu++) {
    if (fs_journal[cpu]->tx_apply_queue.empty())
      continue;

    auto &t = fs_journal[cpu]->tx_apply_queue.front();
    if (t->dependent_txq.empty())
      continue;
    for (auto &d : t->dependent_txq) {
      if (fs_journal[d.id_]->get_applied_tsc() < d.timestamp_)
        cprintf("cpu %d waits for apply on dcpu %d\n", cpu, d.id_);
    }
  }
}

void
print_all_txq_stats()
{
  rootfs_interface->print_txq_stats();
}

bool
mfs_interface::fits_in_journal(size_t num_trans_blocks, int cpu)
{
  // Estimate the space requirements of this transaction in the journal.

  // Check if we can fit num_trans_blocks disk blocks of the transaction
  // as well as the start and commit blocks in the journal. (And also an
  // additional address block if necessary).

  u64 trans_size = num_trans_blocks * BSIZE + 2 * sizeof(journal_header_block)
                   + sizeof(journal_addr_block);

  if (trans_size > PHYS_JOURNAL_SIZE)
    return false;

  if (fs_journal[cpu]->current_offset() + trans_size > PHYS_JOURNAL_SIZE)
    return false;

  return true;
}

void
mfs_interface::write_journal(char *buf, size_t size, transaction *tr, int cpu)
{
  u32 offset = fs_journal[cpu]->current_offset();

  // Make sure we are writing BSIZE bytes at BSIZE-aligned offsets, so that
  // we can skip reading the disk within writei().
  assert(offset % BSIZE == 0 && size == BSIZE);
  assert(writei(sv6_journal[cpu], buf, offset, size, tr) == size);

  offset += size;
  fs_journal[cpu]->update_offset(offset);
}

// Write a transaction's disk blocks to the on-disk journal. The only thing
// remaining to write to the journal on the disk after this function returns,
// would be the commit block.
// Caller must hold ilock for write on sv6_journal.
void
mfs_interface::write_journal_transaction_blocks(
    const std::vector<transaction_diskblock*> &datablocks,
    const u64 timestamp, transaction *trans, int cpu)
{

  journal_header_block hdr_start;
  journal_addr_block hdr_addr;
  memset(&hdr_start, 0, sizeof(hdr_start));
  memset(&hdr_addr, 0, sizeof(hdr_addr));
  hdr_start.timestamp = timestamp;
  hdr_start.header_type = jrnl_start;

  // No. of block addresses that can fit in the start and the address blocks.
  u32 nslots_startblk = sizeof(hdr_start.blocknums) / sizeof(u32);
  u32 nslots_addrblk = sizeof(hdr_addr.blocknums) / sizeof(u32);

  assert(datablocks.size() <= nslots_startblk + nslots_addrblk);

  int count = 0;
  for (auto it = datablocks.begin(); it != datablocks.end(); it++, count++) {

    // Fill the addresses in the start block itself, as far as possible, and use
    // the dedicated address block if it spills over. We won't need more than 1
    // address block, because our journal size is about 4 MB, which limits the
    // number of data blocks for any transaction to about 1024 or so (roughly).
    if (count < nslots_startblk)
      hdr_start.blocknums[count] = (*it)->blocknum;
    else
      hdr_addr.blocknums[count - nslots_startblk] = (*it)->blocknum;
  }

  if (datablocks.size() > nslots_startblk)
    hdr_start.num_addr_blocks = 1;

  // Write out the start block, (the addr block) and the data blocks.

  write_journal((char *)&hdr_start, sizeof(hdr_start), trans, cpu);

  // Write out the address block(s), if we have any.
  if (hdr_start.num_addr_blocks)
    write_journal((char *)&hdr_addr, sizeof(hdr_addr), trans, cpu);

  // Write out the data blocks themselves to the in-memory journal.
  for (auto &b : datablocks)
    write_journal(b->blockdata, BSIZE, trans, cpu);

  // Finally, write the transaction's disk blocks to stable storage (disk).
  trans->write_to_disk_and_flush();
}

// Caller must hold ilock for write on sv6_journal.
void
mfs_interface::write_journal_commit_block(u64 timestamp, int cpu)
{
  // The transaction ends with a commit block containing the same timestamp.
  journal_header_block hdr_commit(timestamp, jrnl_commit);
  transaction *trans = new transaction();

  write_journal((char *)&hdr_commit, sizeof(hdr_commit), trans, cpu);

  trans->write_to_disk_and_flush();
  delete trans;
}

// Called on reboot after a crash. Returns the transaction last committed
// to this journal (but perhaps not yet applied to the disk filesystem).
transaction*
mfs_interface::process_journal(int cpu)
{
  // TODO: Implement crash-recovery for the new journal layout.

#if 0
  u32 offset = 0;
  u64 current_transaction = 0;
  transaction *trans = new transaction(0);
  std::vector<std::unique_ptr<transaction_diskblock> > block_vec;

  size_t hdr_size = sizeof(journal_block_header);
  char hdbuf[hdr_size];
  char hdcmp[hdr_size];
  char databuf[BSIZE];
  bool jrnl_error = false;

  memset(&hdcmp, 0, sizeof(hdcmp));
#endif

  char jrnl_name[32];
  snprintf(jrnl_name, sizeof(jrnl_name), "/sv6journal%d", cpu);
  sv6_journal[cpu] = namei(sref<inode>(), jrnl_name);
  assert(sv6_journal[cpu]);

#if FLASH_FS_AT_BOOT
  // We don't have to look at the journal files since we just flashed a new
  // filesystem to the disk/memory. However, make sure to initialize the inode
  // pointers for the journal files before returning.
  return nullptr;
#endif

#if 0
  ilock(sv6_journal[cpu], WRITELOCK);

  while (!jrnl_error) {

    if (readi(sv6_journal[cpu], hdbuf, offset, hdr_size) != hdr_size)
      break;

    if (!memcmp(hdcmp, hdbuf, hdr_size))
      break;  // Zero-filled block indicates end of journal

    offset += hdr_size;

    if (readi(sv6_journal[cpu], databuf, offset, BSIZE) != BSIZE)
      break;

    offset += BSIZE;

    journal_block_header hd;
    memmove(&hd, hdbuf, sizeof(hd));

    switch (hd.block_type) {

      case jrnl_start:
        current_transaction = hd.timestamp;
        block_vec.clear();
        break;

      case jrnl_data:
        if (hd.timestamp == current_transaction)
          block_vec.push_back(std::make_unique<transaction_diskblock>(hd.blocknum, databuf));
        else
          jrnl_error = true;
        break;

      case jrnl_commit:
        if (hd.timestamp == current_transaction)
          trans->add_blocks(std::move(block_vec));
        else
          jrnl_error = true;
        break;

      default:
        jrnl_error = true;
        break;
    }
  }

  reset_journal(cpu, false); // Don't use async I/O.
  iunlock(sv6_journal[cpu]);

  if (!jrnl_error) {
    trans->enq_tsc = current_transaction;
    return trans;
  }

  return nullptr;
#endif
}

// Reset the journal so that we can start writing to it again, from the
// beginning. Writing a zero header at the very beginning of the journal
// ensures that if we crash and reboot, none of the transactions in the
// journal will be reapplied. Further, when this zero header gets overwritten
// by a subsequent (possibly partially written) transaction, the timestamps
// embedded in each transaction help identify blocks belonging to it, which
// in turn helps us avoid applying partial or corrupted transactions upon
// reboot.
//
// Caller must hold the journal lock and also ilock for write on sv6_journal.
void
mfs_interface::reset_journal(int cpu, bool use_async_io)
{
  fs_journal[cpu]->update_offset(0);

  // TODO: After booting, we might have to really reset the journal (by adding a
  // zero block etc) to avoid unnecessarily reapplying stale committed
  // transactions after a subsequent reboot.
#if 0
  size_t hdr_size = sizeof(journal_block_header);
  char buf[hdr_size];

  memset(buf, 0, sizeof(buf));

  transaction *tr = new transaction(0);

  if (writei(sv6_journal[cpu], buf, 0 /* offset */, hdr_size, tr) != hdr_size)
    panic("reset_journal() failed\n");

  if (use_async_io)
    tr->write_to_disk();
  else
    tr->write_to_disk_raw();
  delete tr;

  fs_journal[cpu]->update_offset(0);
#endif
}

sref<mnode>
mfs_interface::mnode_alloc(u64 inum, u8 mtype)
{
  auto m = root_fs->alloc(mtype);
  inum_to_mnum->insert(inum, m.mn()->mnum_);
  mnum_to_inum->insert(m.mn()->mnum_, inum);
  return m.mn();
}

sref<mnode>
mfs_interface::load_dir_entry(u64 inum, sref<mnode> parent)
{
  u64 mnum;
  sref<mnode> m = mnode_lookup(inum, &mnum);
  if (m)
    return m;

  sref<inode> i = iget(1, inum);
  switch (i->type.load()) {
  case T_DIR:
    m = mnode_alloc(inum, mnode::types::dir);
    break;

  case T_FILE:
    m = mnode_alloc(inum, mnode::types::file);
    break;

  default:
    return sref<mnode>();
  }

  // Link to parent directory created so that the parent's link count is
  // correctly updated.
  if (m->type() == mnode::types::dir) {
    strbuf<DIRSIZ> parent_name("..");
    mlinkref mlink(parent);
    mlink.acquire();
    m->as_dir()->insert(parent_name, &mlink);
  }

  return m;
}

void
mfs_interface::load_dir(sref<inode> i, sref<mnode> m)
{
  dirent de;
  for (size_t pos = 0; pos < i->size; pos += sizeof(de)) {
    assert(sizeof(de) == readi(i, (char*) &de, pos, sizeof(de)));
    if (!de.inum)
      continue;

    sref<mnode> mf = load_dir_entry(de.inum, m);
    if (!mf)
      continue;

    strbuf<DIRSIZ> name(de.name);
    // No links are held to the directory itself (via ".")
    // A link to the parent was already created at the time of mnode creation.
    // The root directory is an exception.
    if (name == "." || (name == ".." && i->inum != 1))
      continue;

    mlinkref mlink(mf);
    mlink.acquire();
    m->as_dir()->insert(name, &mlink);
    mnum_name_insert(mf->mnum_, name);
  }
}

sref<mnode>
mfs_interface::load_root()
{
  scoped_gc_epoch e;
  u64 mnum;
  sref<mnode> m = mnode_lookup(1, &mnum);

  if (m)
    return m;

  sref<inode> i = iget(1, 1);
  assert(i->type.load() == T_DIR);
  m = mnode_alloc(1, mnode::types::dir);

  strbuf<DIRSIZ> name("/");
  mnum_name_insert(m->mnum_, name);
  return m;
}

// Initialize the freeblock_bitmap from the disk when the system boots.
void
mfs_interface::initialize_freeblock_bitmap()
{
  sref<buf> bp;
  int b, bi, nbits;
  superblock sb;
  u32 blocknum, first_free_bblock_bit = 0;

  get_superblock(&sb);

  // Allocate the memory for the bit_vector in one shot, instead of doing it
  // piecemeal using .push_back() in a loop.
  freeblock_bitmap.bit_vector.reserve(sb.size);

  for (b = 0; b < sb.size; b += BPB) {
    blocknum = BBLOCK(b, sb.ninodes);
    bp = buf::get(1, blocknum);
    auto copy = bp->read();

    nbits = std::min((u32)BPB, sb.size - b);

    for (bi = 0; bi < nbits; bi++) {
      int m = 1 << (bi % 8);
      bool f = ((copy->data[bi/8] & m) == 0) ? true : false;

      // Maintain a vector as well as a linked-list representation of the
      // free-bits, to speed up freeing and allocation of blocks, respectively.
      free_bit *bit = new free_bit(b + bi, f);
      freeblock_bitmap.bit_vector.push_back(bit);

      // Make note of the first bitmap block (bit) that starts with a free bit
      // (which is an approximation that, that entire bitmap block (and all
      // the subsequent ones) contains only free bits). That's where we'll
      // start allocating per-CPU resources from (further down in the code),
      // in order to avoid initializing CPU0 with nearly no free bits.
      if (!first_free_bblock_bit && f && bi == 0)
        first_free_bblock_bit = b;
    }
  }

  // Distribute the blocks among the CPUs and add the free blocks to the per-CPU
  // freelists.

  // TODO: Remove this assert and handle cases where multiple CPUs have to share
  // the same bitmap blocks.
  static_assert((NMEGS * BLKS_PER_MEG) / BPB >= NCPU,
                "No. of bitmap-blocks < NCPU\n");

  u32 nbitblocks = sb.size/BPB - first_free_bblock_bit/BPB;
  u32 bitblocks_per_cpu = nbitblocks/NCPU;
  u32 bits_per_cpu = bitblocks_per_cpu * BPB;

  for (int cpu = 0; cpu < NCPU; cpu++) {
    auto list_lock = freeblock_bitmap.freelists[cpu].list_lock.guard();

    if (VERBOSE)
      cprintf("Per-CPU block allocator: CPU %d   blocks [%u - %u]\n",
              cpu, first_free_bblock_bit + cpu * bits_per_cpu,
              first_free_bblock_bit + ((cpu+1) * bits_per_cpu) - 1);

    for (u32 bno = cpu * bits_per_cpu; bno < (cpu+1) * bits_per_cpu; bno++) {
      auto bit = freeblock_bitmap.bit_vector.at(bno + first_free_bblock_bit);
      bit->cpu = cpu;
      if (bit->is_free)
        freeblock_bitmap.freelists[cpu].bit_freelist.push_back(bit);
    }
  }

  // Build a global reserve pool of free blocks using whatever is remaining,
  // to be used when a per-CPU freelist runs out, before stealing free blocks
  // from other CPUs' freelists.
  if (NCPU * bitblocks_per_cpu < nbitblocks) {
    auto list_lock = freeblock_bitmap.reserve_freelist.list_lock.guard();
    for (u32 bno = NCPU * bits_per_cpu; bno + first_free_bblock_bit < sb.size;
         bno++) {
      auto bit = freeblock_bitmap.bit_vector.at(bno + first_free_bblock_bit);
      bit->cpu = NCPU; // Invalid CPU number to denote reserve pool.
      if (bit->is_free)
        freeblock_bitmap.reserve_freelist.bit_freelist.push_back(bit);
    }
  }

  // Any other leftover free bits from [0 to first_free_bblock_bit) also go
  // to the reserve pool. Also make sure to set the CPU number for those bits
  // irrespective of whether they are free.
  {
    auto list_lock = freeblock_bitmap.reserve_freelist.list_lock.guard();
    for (u32 bno = 0; bno < first_free_bblock_bit; bno++) {
      auto bit = freeblock_bitmap.bit_vector.at(bno);
      bit->cpu = NCPU; // Invalid CPU number to denote reserve pool.
      if (bit->is_free)
        freeblock_bitmap.reserve_freelist.bit_freelist.push_back(bit);
    }
  }
}

// Allocate a block from the freeblock_bitmap.
u32
mfs_interface::alloc_block()
{
  u32 bno;
  superblock sb;
  int cpu = myid();
  static bool warned_once = false;

  // Use the linked-list representation of the free-bits to perform block
  // allocation in O(1) time. This list only contains the blocks that are
  // actually free, so we can allocate any one of them.

  {
    auto list_lock = freeblock_bitmap.freelists[cpu].list_lock.guard();

    if (!freeblock_bitmap.freelists[cpu].bit_freelist.empty()) {
      auto it = freeblock_bitmap.freelists[cpu].bit_freelist.begin();
      assert(it->is_free);
      it->is_free = false;
      bno = it->bno_;
      freeblock_bitmap.freelists[cpu].bit_freelist.erase(it);
      return bno;
    }
  }

  // If we run out of blocks in our local CPU's freelist, tap into the global
  // reserve pool first.
  if (VERBOSE && !warned_once) {
    cprintf("WARNING: alloc_block(): CPU %d allocating blocks from the global "
             "reserve pool.\nThis could be a sign that blocks are getting "
             "leaked!\n", cpu);
    warned_once = true;
  }

  // TODO: Allocate from the reserve pool in bulk in order to reduce the
  // chances of contention even further.
  {
    if (freeblock_bitmap.reserve_freelist.bit_freelist.empty())
      goto try_neighbor;

    auto list_lock = freeblock_bitmap.reserve_freelist.list_lock.guard();

    if (!freeblock_bitmap.reserve_freelist.bit_freelist.empty()) {
      auto it = freeblock_bitmap.reserve_freelist.bit_freelist.begin();
      assert(it->is_free);
      it->is_free = false;
      bno = it->bno_;
      freeblock_bitmap.reserve_freelist.bit_freelist.erase(it);
      return bno;
    }
  }

  // We failed to allocate even from the reserve pool. So steal free blocks
  // from other CPUs. Each CPU starts its fallback-search at a different
  // point, in order to avoid hotspots. Note that these blocks are only
  // borrowed temporarily and are prompty returned to the original CPU's
  // freelists upon being freed.
try_neighbor:
  for (int fallback_cpu = cpu + 1; fallback_cpu % NCPU != cpu; fallback_cpu++) {
    int fcpu = fallback_cpu % NCPU;

    if (freeblock_bitmap.freelists[fcpu].bit_freelist.empty())
      continue;

    auto list_lock = freeblock_bitmap.freelists[fcpu].list_lock.guard();

    if (!freeblock_bitmap.freelists[fcpu].bit_freelist.empty()) {
      auto it = freeblock_bitmap.freelists[fcpu].bit_freelist.begin();
      assert(it->is_free);
      it->is_free = false;
      bno = it->bno_;
      freeblock_bitmap.freelists[fcpu].bit_freelist.erase(it);
      return bno;
    }
  }

  panic("alloc_block(): Out of blocks on CPU %d\n", cpu);

  get_superblock(&sb);
  return sb.size; // out of blocks
}

// Mark a block as free in the freeblock_bitmap.
void
mfs_interface::free_block(u32 bno)
{
  // Use the vector representation of the free-bits to free the block in
  // O(1) time (by optimizing the blocknumber-to-free_bit lookup).
  free_bit *bit = freeblock_bitmap.bit_vector.at(bno);

  int cpu = bit->cpu;
  if (cpu < NCPU) {
    auto list_lock = freeblock_bitmap.freelists[cpu].list_lock.guard();
    assert(!bit->is_free);
    bit->is_free = true;
    freeblock_bitmap.freelists[cpu].bit_freelist.push_back(bit);
  } else {
    // This block belongs to the global reserve pool.
    auto list_lock = freeblock_bitmap.reserve_freelist.list_lock.guard();
    assert(!bit->is_free);
    bit->is_free = true;
    freeblock_bitmap.reserve_freelist.bit_freelist.push_back(bit);
  }
}

void
mfs_interface::print_free_blocks(print_stream *s)
{
  percpu<u32> count;
  u32 total_count = 0, reserve_pool_count = 0;

  for (int cpu = 0; cpu < NCPU; cpu++)
    count[cpu] = 0;

  // Traversing the bit_freelist would be faster because they contain only blocks
  // that are actually free. However, to do that we would have to acquire the
  // list_lock, which would prevent concurrent allocations and frees. So go through
  // the bit_vector instead.

  for (auto &b : freeblock_bitmap.bit_vector) {
    if (b->is_free) {
      // No need to re-confirm that it is free with the lock held, since this
      // count is approximate (like a snapshot) anyway.
      if (b->cpu < NCPU)
        count[b->cpu]++;
      else
        reserve_pool_count++;
      total_count++;
    }
  }

  s->println();
  s->print("Total num free blocks: ", total_count);
  s->print(" / ", freeblock_bitmap.bit_vector.size());
  s->println();
  for (int cpu = 0; cpu < NCPU; cpu++) {
    s->print("Num free blocks (CPU ", cpu, "): ", count[cpu]);
    s->println();
  }
  s->println();
  s->print("Num free blocks (Reserve Pool): ", reserve_pool_count);
  s->println();
}

void
mfs_interface::preload_oplog()
{
  // Invoke preload_oplog() on every mfs_log that has been populated in the
  // hash table.
  metadata_log_htab->enumerate([](const u64 &i, mfs_logical_log* &mfs_log)->bool {
    mfs_log->preload_oplog();
    return false;
  });
}

void
kfreeblockprint(print_stream *s)
{
  rootfs_interface->print_free_blocks(s);
}

static int
blkstatsread(mdev*, char *dst, u32 off, u32 n)
{
  window_stream s(dst, off, n);
  kfreeblockprint(&s);
  return s.get_used();
}

void
mfs_interface::reclaim_unreachable_inodes()
{
#if FLASH_FS_AT_BOOT
  // If we just flashed a new filesystem to the disk/memory, there can't be any
  // unreachable inodes to be reclaimed; so don't bother going through the
  // inode list.
  return;
#endif

  int cpu = myid();
  bool do_flush = false;

  superblock sb;
  get_superblock(&sb);

  for (u32 inum = 0; inum < sb.ninodes; inum += IPB) {
    sref<buf> bp = buf::get(1, IBLOCK(inum));
    auto copy = bp->read();

    int ninums = std::min((u32)IPB, sb.ninodes - inum);

    for (int i = 0; i < ninums; i++) {
      const dinode *dip = (const struct dinode*)copy->data + (inum + i)%IPB;

      if (dip->nlink != 0)
        continue;

      u32 dead_inum = inum + i;
      sref<inode> dead_ip = iget(1, dead_inum);

      transaction *tr = new transaction();
      ilock(dead_ip, WRITELOCK);
      itrunc(dead_ip, 0, tr);
      iunlock(dead_ip);

      // TODO: This works fine when deleting files or empty directories, but we
      // will have to do a recursive unlink/delete when dealing with unreachable
      // non-empty directories.
      free_inode(dead_ip, tr);

      add_transaction_to_queue(tr, cpu);
      do_flush = true;
    }
  }

  if (do_flush)
    flush_transaction_queue(cpu, true);
}

// Allocates a lock for every inode block and every bitmap block.
void
mfs_interface::alloc_inodebitmap_locks()
{
  superblock sb;
  get_superblock(&sb);

  // The superblock is immediately followed by the inode blocks, which in turn
  // are immediately followed by the bitmap blocks. So we allocate locks for
  // block numbers 0 through the last bitmap block (inclusive).
  int last_blocknum = BBLOCK(sb.size - 1, sb.ninodes);

  inodebitmap_locks.reserve(last_blocknum + 1);

  for (int i = 0; i <= last_blocknum; i++)
    inodebitmap_locks.push_back(new sleeplock());
}

// Acquire a set of inode-block or bitmap-block locks in the context of the
// specified transaction.
//
// @num_list: List of inode numbers or list of block numbers.
//            (They are distinguished by the type parameter).
// @type: INODE_BLOCK or BITMAP_BLOCK
//
// Note: The numbers in num_list must be uniform - either all of them must be
// inode numbers or all of them must be block numbers.
//
// acquire_inodebitmap_locks() internally calculates the inode-blocks and
// bitmap-blocks corresponding to these numbers and acquires their corresponding
// locks (with appropriate checks to avoid double-acquires).
void
mfs_interface::acquire_inodebitmap_locks(std::vector<u64> &num_list, int type,
                                         transaction *tr)
{
  u32 blocknum = 0;
  superblock sb;
  std::vector<u64> block_numbers;

  switch (type) {
  case INODE_BLOCK:
    for (auto &n : num_list) {
      blocknum = IBLOCK(n);
      for (auto &b : block_numbers) {
        if (b == blocknum)
          goto skip_inode; // Already locked
      }
      block_numbers.push_back(blocknum);
     skip_inode:
      ;
    }

    break;

  case BITMAP_BLOCK:
    get_superblock(&sb);

    for (auto &n : num_list) {
      blocknum = BBLOCK(n, sb.ninodes);
      for (auto &b : block_numbers) {
        if (b == blocknum)
          goto skip_bitmap; // Already locked
      }
      block_numbers.push_back(blocknum);
     skip_bitmap:
      ;
    }

    break;
  }

  // Lock ordering rule: Acquire the locks in increasing order of their
  // block numbers.
  std::sort(block_numbers.begin(), block_numbers.end());
  for (auto &blknum : block_numbers) {
    sleeplock *sl = inodebitmap_locks.at(blknum);
    sl->acquire();
    tr->inodebitmap_locks.push_back(sl);
    tr->inodebitmap_blk_list.push_back(blknum);
  }
}

void
mfs_interface::release_inodebitmap_locks(transaction *tr)
{
  // Lock ordering is irrelevant for release.
  for (auto &sl : tr->inodebitmap_locks)
    sl->release();

  tr->inodebitmap_locks.clear();
  tr->inodebitmap_blk_list.clear();
}

void
initfs()
{
  root_fs = new mfs();
  anon_fs = new mfs();
  rootfs_interface = new mfs_interface();

  // Check all the journals and reapply committed transactions
  transaction *tr;
  std::vector<transaction*> txns_to_apply;
  for (int cpu = 0; cpu < NCPU; cpu++) {
    if ((tr = rootfs_interface->process_journal(cpu)) && tr)
      txns_to_apply.push_back(tr);
  }

  if (!txns_to_apply.empty()) {
    std::sort(txns_to_apply.begin(), txns_to_apply.end(),
              journal::compare_txn_tsc);

    for (auto &tr : txns_to_apply) {
      tr->write_to_disk_update_bufcache();
      delete tr;
    }

    txns_to_apply.clear();
  }

  // If a newly created file (or directory) is fsynced, but its link in the
  // parent is not flushed (by fsyncing the parent directory), the file is
  // potentially unreachable on the disk. If we indeed crash without fsyncing
  // the parent directory, the file (or child directory) must be deleted/
  // reclaimed during crash-recovery upon reboot. A similar situation arises
  // if the last link to an on-disk file or directory is removed (unlinked)
  // but userspace still holds open file descriptors to it at the time of
  // fsync; in that case, its inode cannot be deleted from the disk at the
  // time of fsync, but must be postponed until reboot. We reclaim such
  // dead/unreachable inodes here during reboot.
  rootfs_interface->reclaim_unreachable_inodes();

  // Initialize the free-bit-vector *after* processing the journal,
  // because those transactions could include updates to the free
  // bitmap blocks too!
  rootfs_interface->initialize_freeblock_bitmap();

  rootfs_interface->alloc_inodebitmap_locks();

  devsw[MAJ_BLKSTATS].pread = blkstatsread;
  devsw[MAJ_EVICTCACHES].write = evict_caches;

  root_mnum = rootfs_interface->load_root()->mnum_;
  /* the root mnode gets an extra reference because of its own ".." */
}
