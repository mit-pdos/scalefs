#include "types.h"
#include "kernel.hh"
#include "fs.h"
#include "file.hh"
#include "mnode.hh"
#include "mfs.hh"
#include "scalefs.hh"
#include "kstream.hh"
#include "major.h"

#define min(a, b) ((a) < (b) ? (a) : (b))

// A prime number larger than NINODES (defined in include/fs.h)
#define NINODES_PRIME 1000003

mfs_interface::mfs_interface()
{
  inum_to_mnode = new chainhash<u64, sref<mnode>>(NINODES_PRIME);
  mnum_to_inum = new chainhash<u64, u64>(NINODES_PRIME);
  mnum_to_lock = new chainhash<u64, sleeplock*>(NINODES_PRIME);
  fs_journal = new journal();
  metadata_log_htab = new chainhash<u64, mfs_logical_log*>(NINODES_PRIME);
  alloc_metadata_log(MFS_DELETE_MNUM);
  // XXX(rasha) Set up the physical journal file
}

void
mfs_interface::alloc_mnode_lock(u64 mnum)
{
  mnum_to_lock->insert(mnum, new sleeplock());
}

void
mfs_interface::alloc_metadata_log(u64 mnum)
{
  metadata_log_htab->insert(mnum, new mfs_logical_log());
}

void
mfs_interface::free_inode(u64 mnum, transaction *tr)
{
  sref<inode> ip = get_inode(mnum, "free_inode");

  ilock(ip, 1);
  // Release the inode on the disk.
  ip->type = 0;
  assert(ip->nlink() == 0);
  iupdate(ip, tr);

  // Perform the last decrement of the refcount. This pairs with the
  // extra increment that was done inside inode::init().
  {
    auto w = ip->seq.write_begin();
    ip->dec();
  }
  iunlock(ip);
}

// Returns an sref to an inode if mnum is mapped to one.
sref<inode>
mfs_interface::get_inode(u64 mnum, const char *str)
{
  u64 inum = 0;
  sref<inode> i;

  if (!mnum_to_inum)
    panic("%s: mnum_to_inum mapping does not exist yet", str);

  if (!mnum_to_inum->lookup(mnum, &inum))
    panic("%s: Inode mapping for mnode# %ld does not exist", str, mnum);

  i = iget(1, inum);
  if(!i)
    panic("%s: inode %ld does not exist", str, inum);

  return i;
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

// Flushes out the contents of an in-memory file page to the disk.
int
mfs_interface::sync_file_page(u64 mfile_mnum, char *p, size_t pos,
                              size_t nbytes, transaction *tr)
{
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_mnum, "sync_file_page");
  return writei(i, p, pos, nbytes, tr, true);
}

sref<inode>
mfs_interface::alloc_inode_for_mnode(u64 mnum, u8 type)
{
  sleeplock *mnode_lock;
  assert(mnum_to_lock->lookup(mnum, &mnode_lock));
  auto lk = mnode_lock->guard();

  u64 inum;
  if (inum_lookup(mnum, &inum))
    return iget(1, inum);

  sref<inode> i;
  i = ialloc(1, type);
  iunlock(i);

  inum_to_mnode->insert(i->inum, root_fs->get(mnum));
  mnum_to_inum->insert(mnum, i->inum);

  return i;
}

// Creates a new file [or directory] on the disk if an mnode (mfile) [or mdir]
// does not have a corresponding inode mapping. Returns the inode number of
// the newly created inode.
u64
mfs_interface::create_file_dir_if_new(u64 mnum, u64 parent_mnum, u8 type,
                                      transaction *tr)
{
  u64 inum = 0, parent_inum = 0;
  if (inum_lookup(mnum, &inum)) {
    // A file just needs its inode to be allocated, so it is safe to return
    // here.
    if (type == mnode::types::file)
      return inum;

    // A directory, on the other hand, should additionally have its ".." link
    // initialized. Verify that it has been initialized, and if its not, then
    // continue where we had left off.
    char name[DIRSIZ];
    strcpy(name, "..");
    if (type == mnode::types::dir && dirlookup(iget(1, inum), name))
        return inum;
  }

  // To create a new directory, we need to allocate a new inode as well as
  // initialize it with the ".." link, for which we need to know its parent's
  // inode number.
  if (type == mnode::types::dir && !inum_lookup(parent_mnum, &parent_inum)) {
    sref<inode> parent_i = alloc_inode_for_mnode(parent_mnum, mnode::types::dir);
    parent_inum = parent_i->inum;
  }

  sref<inode> i = alloc_inode_for_mnode(mnum, type);
  if (type == mnode::types::file) {
    ilock(i, 0); // iupdate only reads the inode, so a readlock is sufficient.
    iupdate(i, tr);
  } else if (type == mnode::types::dir) {
    ilock(i, 1);
    dirlink(i, "..", parent_inum, false, tr); // dirlink does an iupdate within.
  }
  iunlock(i);

  return i->inum;
}

// Truncates a file on disk to the specified size (offset).
void
mfs_interface::truncate_file(u64 mfile_mnum, u32 offset, transaction *tr)
{
  scoped_gc_epoch e;

  sref<inode> ip = get_inode(mfile_mnum, "truncate_file");
  ilock(ip, 1);
  itrunc(ip, offset, tr);
  iunlock(ip);

  sref<mnode> m = root_fs->get(mfile_mnum);
  if (m)
    m->as_file()->remove_pgtable_mappings(offset);
}


// Creates a directory entry for a name that exists in the in-memory
// representation but not on the disk.
void
mfs_interface::create_directory_entry(u64 mdir_mnum, char *name, u64 dirent_mnum,
		                      u8 type, transaction *tr)
{
  sref<inode> mdir_i = get_inode(mdir_mnum, "create_directory_entry");

  u64 dirent_inum = create_file_dir_if_new(dirent_mnum, mdir_mnum, type, tr);

  // Check if the directory entry already exists.
  sref<inode> i = dirlookup(mdir_i, name);

  if (i) {
    if (i->inum == dirent_inum)
      return;

    // The name now refers to a different inode. Unlink the old one to make
    // way for a new directory entry for this mapping.
    unlink_old_inode(mdir_mnum, name, tr);
  }

  ilock(mdir_i, 1);
  dirlink(mdir_i, name, dirent_inum, (type == mnode::types::dir)?true:false, tr);
  iunlock(mdir_i);
}

// Deletes directory entries (from the disk) which no longer exist in the mdir.
// The file/directory names that are present in the mdir are specified in names_vec.
void
mfs_interface::unlink_old_inode(u64 mdir_mnum, char* name, transaction *tr)
{
  sref<inode> i = get_inode(mdir_mnum, "unlink_old_inode");
  sref<inode> target = dirlookup(i, name);
  if (!target)
    return;

  ilock(i, 1);
  if (target->type == T_DIR)
    dirunlink(i, name, target->inum, true, tr);
  else
    dirunlink(i, name, target->inum, false, tr);
  iunlock(i);

  // FIXME: The mfs delete transaction depends on hitting mnode::onzero()
  // when its last open file descriptor gets closed. But inum_to_mnode holds
  // an sref to the mnode, so it is unfortunate that we have to prematurely
  // remove the mapping from inum_to_mnode, just to ensure that there is only
  // one outstanding refcount to be dropped, at the time of the last close().
  if (!target->nlink())
    inum_to_mnode->remove(target->inum);

  // Even if the inode's link count drops to zero, we can't actually delete the
  // inode and its file-contents at this point, because userspace might still
  // have open files referring to this inode. We can delete it only after the
  // link count drops to zero *and* all the open files referring to this
  // inode have been closed. Hence, we postpone the delete until the mnode's
  // refcount drops to zero (which satisfies both the above requirements).
}

// Deletes the inode and its file-contents from the disk.
void
mfs_interface::delete_old_inode(u64 mfile_mnum, transaction *tr)
{
  sref<inode> ip = get_inode(mfile_mnum, "delete_old_inode");

  ilock(ip, 1);
  itrunc(ip, 0, tr);
  iunlock(ip);

  free_inode(mfile_mnum, tr);
  mnum_to_inum->remove(mfile_mnum);
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

void
mfs_interface::metadata_op_start(u64 mnum, size_t cpu, u64 tsc_val)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  mfs_log->update_start_tsc(cpu, tsc_val);
}

void
mfs_interface::metadata_op_end(u64 mnum, size_t cpu, u64 tsc_val)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  mfs_log->update_end_tsc(cpu, tsc_val);
}

// Adds a metadata operation to the logical log.
void
mfs_interface::add_to_metadata_log(u64 mnum, mfs_operation *op)
{
  mfs_logical_log *mfs_log;
  assert(metadata_log_htab->lookup(mnum, &mfs_log));
  mfs_log->add_operation(op);
}

// Applies all metadata operations logged in the logical log. Called on sync.
void
mfs_interface::process_metadata_log()
{
#if 0
  mfs_operation_vec ops;
  u64 sync_tsc = 0;
  if (cpuid::features().rdtscp)
    sync_tsc = rdtscp();
  else
    sync_tsc = rdtsc_serialized();
  {
    auto guard = metadata_log->synchronize_upto_tsc(sync_tsc);
    for (auto it = metadata_log->operation_vec.begin(); it !=
      metadata_log->operation_vec.end(); it++)
      ops.push_back(*it);
    metadata_log->operation_vec.clear();
  }

  // If we find create, link, unlink, rename and delete for the same file,
  // absorb all of them and discard those transactions, since the delete
  // cancels out everything else.

  prune_trans_log = new linearhash<u64, mfs_op_idx>(ops.size()*5);
  std::vector<unsigned long> erase_indices;

  // TODO: Handle the scenario where the transaction log contains delete,
  // create and delete for the same mnode.

  for (auto it = ops.begin(); it != ops.end(); ) {
    auto mfs_op_create = dynamic_cast<mfs_operation_create*>(*it);
    auto mfs_op_link   = dynamic_cast<mfs_operation_link*>(*it);
    auto mfs_op_unlink = dynamic_cast<mfs_operation_unlink*>(*it);
    auto mfs_op_rename = dynamic_cast<mfs_operation_rename*>(*it);
    auto mfs_op_delete = dynamic_cast<mfs_operation_delete*>(*it);

    if (mfs_op_create) {

      mfs_op_idx m, mptr;
      m.create_index = it - ops.begin();

      if (prune_trans_log->lookup(mfs_op_create->mnode, &mptr)) {
        panic("process_metadata_log: multiple creates for the same mnode!\n");
      }

      prune_trans_log->insert(mfs_op_create->mnode, m);
      it++;

    } else if (mfs_op_link) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_link->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_link->mnode);
        m = mptr;
        m.link_index = it - ops.begin();
      } else {
        m.link_index = it - ops.begin();
      }

      prune_trans_log->insert(mfs_op_link->mnode, m);
      it++;

    } else if (mfs_op_unlink) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_unlink->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_unlink->mnode);
        m = mptr;
        m.unlink_index = it - ops.begin();
      } else {
        m.unlink_index = it - ops.begin();
      }

      prune_trans_log->insert(mfs_op_unlink->mnode, m);
      it++;

    } else if (mfs_op_rename) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_rename->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_rename->mnode);
        m = mptr;
        m.rename_index = it - ops.begin();
      } else {
        m.rename_index = it - ops.begin();
      }

      prune_trans_log->insert(mfs_op_rename->mnode, m);
      it++;

    } else if (mfs_op_delete) {

      mfs_op_idx m, mptr;
      if (prune_trans_log->lookup(mfs_op_delete->mnode, &mptr)) {
        prune_trans_log->remove(mfs_op_delete->mnode);
        m = mptr;

        m.delete_index = it - ops.begin();

        // Absorb only if the corresponding create is also found.
        if (m.create_index != -1) {
          erase_indices.push_back(m.create_index);

          if (m.link_index != -1)
            erase_indices.push_back(m.link_index);

          if (m.unlink_index != -1)
            erase_indices.push_back(m.unlink_index);

          if (m.rename_index != -1)
            erase_indices.push_back(m.rename_index);

          erase_indices.push_back(m.delete_index);

          it++;

        } else {
          // If we didn't find the create, we should execute all the
          // transactions.
          it++;
        }

      } else {

        m.delete_index = it - ops.begin();
        prune_trans_log->insert(mfs_op_delete->mnode, m);
        it++;
      }

    }
  }

  std::sort(erase_indices.begin(), erase_indices.end(),
            std::greater<unsigned long>());

  for (auto &idx : erase_indices)
    ops.erase(ops.begin() + idx);

  delete prune_trans_log;

  for (auto it = ops.begin(); it != ops.end(); it++) {
    transaction *tr = new transaction((*it)->timestamp);
    (*it)->apply(tr);
    add_to_journal_locked(tr);
    delete (*it);
  }
#endif
}

void
mfs_interface::process_metadata_log_and_flush()
{
#if 0
  auto journal_lock = fs_journal->prepare_for_commit();
  process_metadata_log();
  flush_journal_locked();
#endif
}

void
mfs_interface::sync_dirty_files()
{
#if 0
  superblock sb;

  get_superblock(&sb);

  // Invoke sync_file() for every file mnode that we know of, by evaluating
  // every key-value pair in the hash-table. This scheme (of using enumerate()
  // with a callback) is more efficient than doing lookups for all inodes from
  // 0 through sb.ninodes in the hash-table.

  inum_to_mnode->enumerate([](const u64 &inum, sref<mnode> &m)->bool {
    if (m && m->type() == mnode::types::file)
      m->as_file()->sync_file(false);

    return false;
  });
#endif
}

void
mfs_interface::evict_bufcache()
{
  superblock sb;

  cprintf("evict_caches: dropping buffer-cache blocks\n");

  get_superblock(&sb);

  for (u64 inum = 0; inum < sb.ninodes; inum++) {
    sref<mnode> m;

    if (inum_to_mnode->lookup(inum, &m) && m) {
      if(m->type() == mnode::types::file) {
        sref<inode> ip = get_inode(m->mnum_, "evict_bufcache");
        drop_bufcache(ip);
      }
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
    sref<mnode> m;

    if (inum_to_mnode->lookup(inum, &m) && m) {
      if (m->type() == mnode::types::file) {
          // Skip uninitialized files, as they won't have any page-cache
          // pages yet. Moreover, file initialization itself consumes
          // some memory (for the radix array), which is undesirable here.
          if (m->is_initialized())
            m->as_file()->drop_pagecache();
      }
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
mfs_interface::apply_rename_pair(std::vector<rename_metadata> &rename_stack)
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
    transaction *tr = nullptr;

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

    // Make sure that both parts of the rename operation are applied within
    // the same transaction, to preserve atomicity.
    tr = new transaction(link_op->timestamp);
    add_op_to_journal(link_op, tr);
    add_op_to_journal(unlink_op, tr);

    // Now we need to delete these two sub-operations from their oplogs.
    // Luckily, we know that as of this moment, both these rename sub-
    // operations are at the beginning of their oplogs (because we have
    // already applied their predecessor operations).
    mfs_log_src->operation_vec.erase(mfs_log_src->operation_vec.begin());
    mfs_log_dst->operation_vec.erase(mfs_log_dst->operation_vec.begin());

  unlock:
    ; // release the locks held by src_guard and dst_guard
  }

  if (dst_mnum != src_mnum)
    mfs_log_dst->lock.release();

  mfs_log_src->lock.release();
}

void
mfs_interface::add_op_to_journal(mfs_operation *op, transaction *tr)
{
  if (!tr)
    tr = new transaction(op->timestamp);

  auto journal_lock = fs_journal->prepare_for_commit();
  op->apply(tr);
  add_to_journal_locked(tr);
  delete op;
}

// Return values:
// 0 - All done (processed operations upto max_tsc in the given mfs_log)
// 1 - Encountered a new rename sub-operation and added its counterpart to the
//     pending stack as a dependency.
// 2 - Got a counterpart for a rename sub-operation, which completes the pair
int
mfs_interface::process_ops_from_oplog(mfs_logical_log *mfs_log, u64 max_tsc,
                                      std::vector<mnum_tsc> &pending_stack,
                                      std::vector<rename_metadata> &rename_stack)
{
  // Synchronize the oplog loggers.
  auto guard = mfs_log->synchronize_upto_tsc(max_tsc);

  if (!mfs_log->operation_vec.size())
    return 0;

  for (auto it = mfs_log->operation_vec.begin();
       it != mfs_log->operation_vec.end(); ) {

    auto rename_link_op = dynamic_cast<mfs_operation_rename_link*>(*it);
    auto rename_unlink_op = dynamic_cast<mfs_operation_rename_unlink*>(*it);

    if (rename_link_op || rename_unlink_op) {

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
        // dependency.
        pending_stack.push_back({rename_link_op->src_parent_mnum,
                                 rename_link_op->timestamp});
      } else if (rename_unlink_op) {
        rename_stack.push_back({rename_unlink_op->src_parent_mnum,
                                rename_unlink_op->dst_parent_mnum,
                                rename_unlink_op->timestamp});
        // We have the unlink part of the rename, so add the link part as a
        // dependency.
        pending_stack.push_back({rename_unlink_op->dst_parent_mnum,
                                 rename_unlink_op->timestamp});
      }

      if (rename_timestamp && (*it)->timestamp == rename_timestamp)
        return 2;
      return 1;
    }

    add_op_to_journal(*it);
    it = mfs_log->operation_vec.erase(it);
  }

  return 0;
}

// Applies metadata operations logged in the logical journal. Called on
// fsync to resolve any metadata dependencies.
void
mfs_interface::process_metadata_log(u64 max_tsc, u64 mnode_mnum, bool isdir)
{
  std::vector<rename_metadata> rename_stack;
  std::vector<mnum_tsc> pending_stack;
  mfs_logical_log *mfs_log;
  int ret;

  pending_stack.push_back({mnode_mnum, max_tsc});

  while (pending_stack.size()) {
    mnum_tsc mt = pending_stack.back();
    mnode_mnum = mt.mnum;
    max_tsc = mt.tsc;

    assert(metadata_log_htab->lookup(mnode_mnum, &mfs_log));

    mfs_log->lock.acquire();
    ret = process_ops_from_oplog(mfs_log, max_tsc, pending_stack, rename_stack);
    mfs_log->lock.release();

    switch (ret) {

    // 0 - All done (processed operations upto max_tsc in the given mfs_log)
    case 0:
      pending_stack.pop_back();
      break;

    // 1 - Encountered a new rename sub-operation and added its counterpart
    //     to the pending stack as a dependency.
    case 1:
      continue;

    // 2 - Got a counterpart for a rename sub-operation, which completes the
    //     pair. So acquire the necessary locks and apply both parts of the
    //     rename atomically using a single transaction.
    case 2:
      apply_rename_pair(rename_stack);
      // Since the rename sub-operations got paired up and were applied, we
      // don't have to process the other directory any further for this fsync
      // call. So pop it off the pending stack.
      pending_stack.pop_back();
      break;

    default:
      panic("Got invalid return code from process_ops_from_oplog()");
    }
  }

  assert(!rename_stack.size() && !pending_stack.size());
}

void
mfs_interface::process_metadata_log_and_flush(u64 max_tsc, u64 mnum, bool isdir)
{
  process_metadata_log(max_tsc, mnum, isdir);
  auto journal_lock = fs_journal->prepare_for_commit();
  flush_journal_locked();
}

// Create operation
void
mfs_interface::mfs_create(mfs_operation_create *op, transaction *tr)
{
  scoped_gc_epoch e;
  create_file_dir_if_new(op->mnode_mnum, op->parent_mnum, op->mnode_type, tr);
}

// Link operation
void
mfs_interface::mfs_link(mfs_operation_link *op, transaction *tr)
{
  scoped_gc_epoch e;
  create_directory_entry(op->parent_mnum, op->name, op->mnode_mnum,
                         op->mnode_type, tr);
}

// Unlink operation
void
mfs_interface::mfs_unlink(mfs_operation_unlink *op, transaction *tr)
{
  scoped_gc_epoch e;
  char str[DIRSIZ];
  strcpy(str, op->name);
  unlink_old_inode(op->parent_mnum, str, tr);
}

// Delete operation
void
mfs_interface::mfs_delete(mfs_operation_delete *op, transaction *tr)
{
  scoped_gc_epoch e;

  delete_old_inode(op->mnode_mnum, tr);
}

// Rename Link operation
void
mfs_interface::mfs_rename_link(mfs_operation_rename_link *op, transaction *tr)
{
  scoped_gc_epoch e;
  create_directory_entry(op->dst_parent_mnum, op->newname, op->mnode_mnum,
                         op->mnode_type, tr);
}

// Rename Unlink operation
void
mfs_interface::mfs_rename_unlink(mfs_operation_rename_unlink *op, transaction *tr)
{
  scoped_gc_epoch e;
  char str[DIRSIZ];
  strcpy(str, op->name);

  unlink_old_inode(op->src_parent_mnum, str, tr);
}

// Logs a transaction to the physical journal. Does not apply it to the disk yet
void
mfs_interface::add_to_journal_locked(transaction *tr)
{
  fs_journal->add_transaction_locked(tr);
}

void
mfs_interface::pre_process_transaction(transaction *tr)
{
  // Update the free bitmap on the disk.
  balloc_on_disk(tr->allocated_block_list, tr);
  bfree_on_disk(tr->free_block_list, tr);
}

void
mfs_interface::post_process_transaction(transaction *tr)
{
  // Now that the transaction has been committed, mark the freed blocks as
  // free in the in-memory free-bit-vector.
  for (auto f = tr->free_block_list.begin();
       f != tr->free_block_list.end(); f++)
    free_block(*f);
}

void
mfs_interface::apply_trans_on_disk(transaction *tr)
{
  // This transaction has been committed to the journal. Writeback the changes
  // to the original locations on the disk.
  for (auto b = tr->blocks.begin(); b != tr->blocks.end(); b++)
    (*b)->writeback_async();

  for (auto b = tr->blocks.begin(); b != tr->blocks.end(); b++)
    (*b)->async_iowait();
}

// Logs a transaction in the disk journal and then applies it to the disk,
// if flush_journal is set to true.
void
mfs_interface::add_fsync_to_journal(transaction *tr, bool flush_journal)
{
  auto journal_lock = fs_journal->prepare_for_commit();

  if (!flush_journal) {
    add_to_journal_locked(tr);
    return;
  }

  u64 timestamp = tr->timestamp_;
  transaction *trans;

  pre_process_transaction(tr);

  tr->prepare_for_commit();
  tr->deduplicate_blocks();

  trans = new transaction(0);

  write_journal_trans_prolog(timestamp, trans);

  // Write out the transaction blocks to the disk journal in timestamp order.
  write_journal_transaction_blocks(tr->blocks, timestamp, trans);

  write_journal_trans_epilog(timestamp, trans); // This also deletes trans.

  post_process_transaction(tr);
  apply_trans_on_disk(tr);

  ideflush();

  // The blocks have been written to disk successfully. Safe to delete
  // this transaction from the journal. (This means that all the
  // transactions till this point have made it to the disk. So the journal
  // can simply be truncated.) Since the journal is static, the journal file
  // simply needs to be zero-filled.)
  clear_journal();
}

// Writes out the physical journal to the disk, and applies the committed
// transactions to the disk filesystem.
void
mfs_interface::flush_journal_locked()
{
  u64 timestamp = 0, prolog_timestamp = 0;
  transaction *trans, *prune_trans;

  // A vector of processed transactions, which need to be applied later
  // (post-processed).
  std::vector<transaction*> processed_trans_vec;

  if (fs_journal->transaction_log.size() == 0)
    return; // Nothing to do.

  trans = new transaction(0);

  // A transaction to prune out multiple updates to the same disk block
  // from multiple sub-transactions. It merges all of them into 1 disk
  // block update.
  prune_trans = new transaction(0);

  {
    auto it = fs_journal->transaction_log.begin();
    prolog_timestamp = (*it)->timestamp_;
    write_journal_trans_prolog(prolog_timestamp, trans);
  }

  for (auto it = fs_journal->transaction_log.begin();
       it != fs_journal->transaction_log.end(); it++) {

    timestamp = (*it)->timestamp_;
    pre_process_transaction(*it);

    retry:
    (*it)->prepare_for_commit();

    if (fits_in_journal((*it)->blocks.size())) {

      prune_trans->add_blocks(std::move((*it)->blocks));

      processed_trans_vec.push_back(*it);

    } else {

      // No space left in the journal to accommodate this sub-transaction.
      // So commit and apply all the earlier sub-transactions, to make space
      // for the remaining sub-transactions.

      // Explicitly release this sub-transaction's write_lock. We'll retry this
      // sub-transaction later.
      (*it)->finish_after_commit();

      prune_trans->deduplicate_blocks();

      // Write out the transaction blocks to the disk journal in timestamp order.
      write_journal_transaction_blocks(prune_trans->blocks, timestamp, trans);

      write_journal_trans_epilog(prolog_timestamp, trans); // This also deletes trans.

      // Apply all the committed sub-transactions to their final destinations
      // on the disk.
      for (auto t = processed_trans_vec.begin();
           t != processed_trans_vec.end(); t++) {

        post_process_transaction(*t);

      }

      apply_trans_on_disk(prune_trans);
      ideflush();

      processed_trans_vec.clear();
      clear_journal();

      // Retry this sub-transaction, since we couldn't write it to the journal.
      delete prune_trans;
      prune_trans = new transaction(0);
      trans = new transaction(0);
      prolog_timestamp = timestamp;
      write_journal_trans_prolog(prolog_timestamp, trans);
      goto retry;
    }

  }

  // Finalize and flush out any remaining transactions from the journal.

  if (!processed_trans_vec.empty()) {

      prune_trans->deduplicate_blocks();

      // Write out the transaction blocks to the disk journal in timestamp order.
      write_journal_transaction_blocks(prune_trans->blocks, timestamp, trans);
  }

  write_journal_trans_epilog(prolog_timestamp, trans); // This also deletes trans.

  // Apply all the committed sub-transactions to their final destinations on
  // the disk.
  for (auto t = processed_trans_vec.begin();
       t != processed_trans_vec.end(); t++) {

    post_process_transaction(*t);

  }

  apply_trans_on_disk(prune_trans);
  ideflush();

  processed_trans_vec.clear();

  // The blocks have been written to disk successfully. Safe to delete
  // this transaction from the journal. (This means that all the
  // transactions till this point have made it to the disk. So the journal
  // can simply be truncated.) Since the journal is static, the journal file
  // simply needs to be zero-filled.)
  clear_journal();

  delete prune_trans;

  for (auto it = fs_journal->transaction_log.begin();
       it != fs_journal->transaction_log.end(); it++) {

    delete (*it);
  }

  fs_journal->transaction_log.clear();
}

void
mfs_interface::write_journal_hdrblock(const char *header, const char *datablock,
                                      transaction *tr)
{
  size_t data_size = BSIZE;
  size_t hdr_size = sizeof(journal_block_header);
  u32 offset = fs_journal->current_offset();

  if (writei(sv6_journal, header, offset, hdr_size, tr) != hdr_size)
    panic("Journal write (header block) failed\n");

  offset += hdr_size;

  if (writei(sv6_journal, datablock, offset, data_size, tr) != data_size)
    panic("Journal write (data block) failed\n");

  offset += data_size;

  fs_journal->update_offset(offset);
}

void
mfs_interface::write_journal_header(u8 hdr_type, u64 timestamp,
                                    transaction *trans)
{
  char databuf[BSIZE];
  char buf[sizeof(journal_block_header)];

  journal_block_header hdstart(timestamp, 0, jrnl_start);
  journal_block_header hdcommit(timestamp, 0, jrnl_commit);

  memset(buf, 0, sizeof(buf));
  memset(databuf, 0, sizeof(databuf));

  switch (hdr_type) {
    case jrnl_start:
      memmove(buf, (void *) &hdstart, sizeof(hdstart));
      write_journal_hdrblock(buf, databuf, trans);
      break;

    case jrnl_commit:
      memmove(buf, (void *) &hdcommit, sizeof(hdcommit));
      write_journal_hdrblock(buf, databuf, trans);
      break;

    default:
      cprintf("write_journal_header: requested invalid header %u\n", hdr_type);
      break;
  }
}

bool
mfs_interface::fits_in_journal(size_t num_trans_blocks)
{
  // Estimate the space requirements of this transaction in the journal.

  u64 trans_size;
  size_t hdr_size = sizeof(journal_block_header);
  u32 offset = fs_journal->current_offset();

  // The start block for this transaction has already been written to the
  // journal. So we now need space to write num_trans_blocks disk blocks
  // of the transaction and the final commit block.
  trans_size = (hdr_size + BSIZE) * (1 + num_trans_blocks);

  if (offset + trans_size > PHYS_JOURNAL_SIZE) {
    // No space left in the journal.
    return false;
  }

  return true;
}


void
mfs_interface::write_journal_trans_prolog(u64 timestamp, transaction *trans)
{
  // A transaction begins with a start block.
  write_journal_header(jrnl_start, timestamp, trans);
}

// Write a transaction's disk blocks to the journal in memory. Don't write
// or flush it to the disk yet.
void
mfs_interface::write_journal_transaction_blocks(
    const std::vector<std::unique_ptr<transaction_diskblock> >& vec,
    const u64 timestamp, transaction *trans)
{
  assert(sv6_journal);

  size_t hdr_size = sizeof(journal_block_header);
  char buf[hdr_size];

  // Write out the transaction diskblocks.
  for (auto it = vec.begin(); it != vec.end(); it++) {

    journal_block_header hddata(timestamp, (*it)->blocknum, jrnl_data);

    memmove(buf, (void *) &hddata, sizeof(hddata));
    write_journal_hdrblock(buf, (*it)->blockdata, trans);
  }
}

void
mfs_interface::write_journal_trans_epilog(u64 timestamp, transaction *trans)
{
  // Write out the disk blocks in the transaction to stable storage before
  // committing the transaction.
  trans->write_to_disk();
  delete trans;

  // The transaction ends with a commit block.
  trans = new transaction(0);

  write_journal_header(jrnl_commit, timestamp, trans);

  trans->write_to_disk();
  delete trans;
}

// Called on reboot after a crash. Applies committed transactions.
void
mfs_interface::process_journal()
{
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

  sv6_journal = namei(sref<inode>(), "/sv6journal");
  assert(sv6_journal);

  ilock(sv6_journal, 1);

  while (!jrnl_error) {

    if (readi(sv6_journal, hdbuf, offset, hdr_size) != hdr_size)
      break;

    if (!memcmp(hdcmp, hdbuf, hdr_size))
      break;  // Zero-filled block indicates end of journal

    offset += hdr_size;

    if (readi(sv6_journal, databuf, offset, BSIZE) != BSIZE)
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

  // Zero-fill the journal
  zero_fill(sv6_journal, PHYS_JOURNAL_SIZE);
  iunlock(sv6_journal);

  trans->write_to_disk_update_bufcache();
  delete trans;
}

// Clear (zero-fill) the journal file on the disk
void
mfs_interface::clear_journal()
{
  assert(sv6_journal);
  ilock(sv6_journal, 1);
  zero_fill(sv6_journal, fs_journal->current_offset());
  iunlock(sv6_journal);
  fs_journal->update_offset(0);
}

bool
mfs_interface::inum_lookup(u64 mnum, u64 *inum)
{
  if (!mnum_to_inum)
    panic("mnum_to_inum mapping does not exist yet");
  if (mnum_to_inum->lookup(mnum, inum))
    return true;
  return false;
}

sref<mnode>
mfs_interface::mnode_alloc(u64 inum, u8 mtype)
{
  auto m = root_fs->alloc(mtype);
  inum_to_mnode->insert(inum, m.mn());
  if (!mnum_to_inum)
    panic("mnum_to_inum mapping does not exist yet");
  mnum_to_inum->insert(m.mn()->mnum_, inum);
  return m.mn();
}

sref<mnode>
mfs_interface::load_dir_entry(u64 inum, sref<mnode> parent)
{
  sref<mnode> m;
  if (inum_to_mnode->lookup(inum, &m))
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
  }
}

sref<mnode>
mfs_interface::load_root()
{
  scoped_gc_epoch e;
  sref<mnode> m;
  if (inum_to_mnode->lookup(1, &m))
    return m;

  sref<inode> i = iget(1, 1);
  assert(i->type.load() == T_DIR);
  m = mnode_alloc(1, mnode::types::dir);
  return m;
}

// Initialize the free bit vector from the disk when the system boots.
void
mfs_interface::initialize_free_bit_vector()
{
  sref<buf> bp;
  int b, bi, nbits;
  u32 blocknum;
  superblock sb;

  get_superblock(&sb);

  // Allocate the memory for free_bit_vector in one shot, instead of doing it
  // piecemeal using .emplace_back() in a loop.
  free_bit_vector.reserve(sb.size);

  for (b = 0; b < sb.size; b += BPB) {
    blocknum = BBLOCK(b, sb.ninodes);
    bp = buf::get(1, blocknum);
    auto copy = bp->read();

    nbits = min(BPB, sb.size - b);

    for (bi = 0; bi < nbits; bi++) {
      int m = 1 << (bi % 8);
      bool f = ((copy->data[bi/8] & m) == 0) ? true : false;

      // Maintain a vector as well as a linked-list representation of the
      // free-bits, to speed up freeing and allocation of blocks, respectively.
      free_bit *bit = new free_bit(b + bi, f);
      free_bit_vector.emplace_back(bit);

      if (!f)
        continue;

      // Add the block to the freelist if it is actually free.
      auto list_lock = freelist_lock.guard();
      free_bit_freelist.push_back(bit);
    }
  }
}

// Return the block number of a free block in the free_bit_vector.
u32
mfs_interface::alloc_block()
{
  u32 bno;
  superblock sb;

  // Use the linked-list representation of the free-bits to perform block
  // allocation in O(1) time. This list only contains the blocks that are
  // actually free, so we can allocate any one of them.

  auto list_lock = freelist_lock.guard();

  if (!free_bit_freelist.empty()) {

    auto it = free_bit_freelist.begin();
    auto lock = it->write_lock.guard();

    assert(it->is_free == true);
    it->is_free = false;
    bno = it->bno_;
    free_bit_freelist.erase(it);

    return bno;
  }

  get_superblock(&sb);
  return sb.size; // out of blocks
}

// Mark a block as free in the free_bit_vector.
void
mfs_interface::free_block(u32 bno)
{
  // Use the vector representation of the free-bits to free the block in
  // O(1) time (by optimizing the blocknumber-to-free_bit lookup).
  free_bit *bit = free_bit_vector.at(bno);

  if (bit->is_free)
    panic("freeing free block %u\n", bno);

  {
    auto lock = bit->write_lock.guard();
    bit->is_free = true;
  }

  // Drop the write_lock before taking the freelist_lock, to avoid a
  // potential ABBA deadlock with alloc_block().

  // Add it to the free_bit_freelist.
  auto list_lock = freelist_lock.guard();
  free_bit_freelist.push_front(bit);
}

void
mfs_interface::print_free_blocks(print_stream *s)
{
  u32 count = 0;

  // Traversing the free_bit_freelist would be faster because they contain
  // only blocks that are actually free. However, to do that we would have
  // to acquire the freelist_lock, which would prevent concurrent allocations.
  // Hence go through the free_bit_vector instead.
  for (auto it = free_bit_vector.begin(); it != free_bit_vector.end();
       it++) {

    if ((*it)->is_free) {
      // No need to re-confirm that it is free with the lock held, since this
      // count is approximate (like a snapshot) anyway.
      count++;
    }
  }

  s->println();
  s->print("Num free blocks: ", count);
  s->print(" / ", free_bit_vector.size());
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
initfs()
{
  root_fs = new mfs();
  anon_fs = new mfs();
  rootfs_interface = new mfs_interface();

  // Check the journal and reapply committed transactions
  rootfs_interface->process_journal();

  // Initialize the free-bit-vector *after* processing the journal,
  // because those transactions could include updates to the free
  // bitmap blocks too!
  rootfs_interface->initialize_free_bit_vector();

  devsw[MAJ_BLKSTATS].pread = blkstatsread;
  devsw[MAJ_EVICTCACHES].write = evict_caches;

  root_mnum = rootfs_interface->load_root()->mnum_;
  /* the root mnode gets an extra reference because of its own ".." */
}
