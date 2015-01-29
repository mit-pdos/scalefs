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

mfs_interface::mfs_interface() {
  inum_to_mnode = new linearhash<u64, sref<mnode>>(4099);
  mnode_to_inode = new linearhash<u64, u64>(4099);
  fs_journal = new journal();
  metadata_log = new mfs_logical_log();
  // XXX(rasha) Set up the physical journal file
}

// Returns an sref to an inode if mnode_inum is mapped to one.
sref<inode> mfs_interface::get_inode(u64 mnode_inum, const char *str) {
  u64 inum = 0;
  sref<inode> i;
  if (!mnode_to_inode)
    panic("%s: mnode_to_inode mapping does not exist yet", str);
  if (!mnode_to_inode->lookup(mnode_inum, &inum))
    panic("%s: Mapping for mnode# %ld does not exist", str, mnode_inum);
  i = iget(1, inum);
  if(!i)
    panic("%s: inode %ld does not exist", str, inum);
  return i;
}

// Initializes the size of an mfile to the on-disk file size. This helps the
// mfile distinguish between when a file page has to be demand-laoded from the
// disk and when a new page has to be allocated. Called the first time the mfile
// is referred to.
void mfs_interface::initialize_file(sref<mnode> m) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(m->inum_, "initialize_file");
  auto resizer = m->as_file()->write_size();
  resizer.initialize_from_disk(i->size);
}

// Reads in a file page from the disk.
int mfs_interface::load_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "load_file_page");
  return readi(i, p, pos, nbytes);
}

// Reads the on-disk file size.
u64 mfs_interface::get_file_size(u64 mfile_inum) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "get_file_size");
  return i->size;
}

// Updates the file size on the disk.
void mfs_interface::update_file_size(u64 mfile_inum, u32 size, transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "update_file_size");
  update_size(i, size, tr); 
}

// Flushes out the contents of an in-memory file page to the disk.
int mfs_interface::sync_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes,
    transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "sync_file_page");
  return writei(i, p, pos, nbytes, tr, true);
}

// Creates a new file on the disk if an mnode (mfile) does not have a corresponding
// inode mapping.
u64 mfs_interface::create_file_if_new(u64 mfile_inum, u64 parent, u8 type,
  char *name, transaction *tr, bool link_in_parent) {
  u64 inum = 0, parent_inum = 0, returnval = 0;
  if (inode_lookup(mfile_inum, &inum))
    return 0;

  // The parent directory will always be present on the disk when the child is
  // created. This is because all create operations are logged in the logical
  // log (metadata operations). A parent's create will have occurred before the
  // child's create. This is the order the operations will be present in the
  // logical log and hence this is the order they'll make it to the disk. This
  // gets rid of the scenario where we would need to go up the directory tree
  // and explicitly sync all new ancestors.
  if (!inode_lookup(parent, &parent_inum))
    panic("create_file_if_new: parent %ld does not exist\n", parent);
    
  sref<inode> i;
  i = ialloc(1, type);
  mnode_to_inode->insert(mfile_inum, i->inum);
  inum_to_mnode->insert(i->inum, root_fs->get(mfile_inum));
  returnval = i->inum;
  iupdate(i, tr);
  iunlock(i);

  // If link_in_parent flag is set, create a directory entry in the parent
  // directory corresponding to this file. By default we always create directory
  // entries in the parent directory for newly-created files that are fsynced.
  // POSIX does not require this however.
  if (link_in_parent) {
    sref<inode> parenti = iget(1, parent_inum);
    if (!parenti)
      panic("create_file_if_new: parent %ld does not exist on disk\n",
        parent_inum);
    ilock(parenti, 1);
    dirlink(parenti, name, i->inum, false);
    dir_flush(parenti, tr);
    iunlock(parenti);
  }

  return returnval;
}

// Truncates a file on disk to the specified size (offset).
void mfs_interface::truncate_file(u64 mfile_inum, u32 offset, transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "truncate_file");
  itrunc(i, offset, tr);
  sref<mnode> m = root_fs->get(mfile_inum);
  if (m)
    m->as_file()->remove_pgtable_mappings(offset);
}

// Creates a new direcotry on the disk if an mnode (mdir) does not have a 
// corresponding inode mapping.
u64 mfs_interface::create_dir_if_new(u64 mdir_inum, u64 parent, u8 type,
  char *name, transaction *tr, bool link_in_parent) {
  u64 inum = 0, parent_inum = 0, returnval = 0;
  if (inode_lookup(mdir_inum, &inum))
    return 0;

  // The parent directory will always be present on the disk when the child is
  // created. This is because all create operations are logged in the logical
  // log (metadata operations). A parent's create will have occurred before the
  // child's create. This is the order the operations will be present in the
  // logical log and hence this is the order they'll make it to the disk. This
  // gets rid of the scenario where we would need to go up the directory tree
  // and explicitly sync all new ancestors.
  if (!inode_lookup(parent, &parent_inum))
    panic("create_dir_if_new: parent %ld does not exist\n", parent);

  sref<inode> i, parenti;
  i = ialloc(1, type);
  mnode_to_inode->insert(mdir_inum, i->inum);
  inum_to_mnode->insert(i->inum, root_fs->get(mdir_inum));
  returnval = i->inum;
  dirlink(i, "..", parent_inum, true);
  dir_flush(i, tr);
  iunlock(i);

  // If link_in_parent flag is set, create a directory entry in the parent
  // directory corresponding to this child directory. By default we always
  // create directory entries in the parent directory for newly-created
  // directories that are fsynced. POSIX does not require this however.
  if (link_in_parent) {
    parenti = iget(1, parent_inum);
    ilock(parenti, 1);
    dirlink(parenti, name, i->inum, true);
    dir_flush(parenti, tr);
    iunlock(parenti);
  }

  return returnval;
}

// Creates a directory entry for a name that exists in the in-memory 
// representation but not on the disk.
void mfs_interface::create_directory_entry(u64 mdir_inum, char *name, u64
    dirent_inum, u8 type, transaction *tr) {
  sref<inode> i = get_inode(mdir_inum, "create_directory_entry");

  sref<inode> di = dirlookup(i, name);
  if (di) {
    // directory entry exists
    if (di->inum == dirent_inum)
      return;
    // The name now refers to a different inode. Unlink the old one and create a
    // new directory entry for this mapping.
    if(di->type == T_DIR)
      dirunlink(i, name, di->inum, true);
    else
      dirunlink(i, name, di->inum, false);
    if(!di->nlink()) {
      ilock(di, 1);
      itrunc(di, 0, tr);
      iunlock(di);
      inum_to_mnode->remove(di->inum);
    }
  }

  ilock(i, 1);
  u64 inum = 0;
  inode_lookup(dirent_inum, &inum);
  if (inum) { // inode exists. Just create a dir entry. No need to allocate
    dirlink(i, name, inum, (type == mnode::types::dir)?true:false);
  } else {  // allocate new inode
    if (type == mnode::types::file) {
      inum = create_file_if_new(dirent_inum, mdir_inum, type, name, tr, false);
      dirlink(i, name, inum, false);
    } else if (type == mnode::types::dir) {
      inum = create_dir_if_new(dirent_inum, mdir_inum, type, name, tr, false);
      dirlink(i, name, inum, true);
    }
  } 
  iunlock(i);
}

// Deletes directory entries (from the disk) which no longer exist in the mdir.
// The file/directory names that are present in the mdir are specified in names_vec.
void mfs_interface::unlink_old_inode(u64 mdir_inum, char* name, transaction *tr) {
  sref<inode> i = get_inode(mdir_inum, "unlink_old_inode");
  sref<inode> target = dirlookup(i, name);
  if (!target)
    return;
  if (target->type == T_DIR)
    dirunlink(i, name, target->inum, true);
  else
    dirunlink(i, name, target->inum, false);
  if (!target->nlink()) {
    ilock(target, 1);
    itrunc(target, 0, tr);
    iunlock(target);
    inum_to_mnode->remove(target->inum);
  }
}

// Calls a dir_flush on the directory.
void mfs_interface::update_dir_inode(u64 mdir_inum, transaction *tr) {
  sref<inode> i = get_inode(mdir_inum, "update_dir_inode");
  update_dir(i, tr);
}

// Initializes the mdir the first time it is referred to. Populates directory
// entries from the disk.
void mfs_interface::initialize_dir(sref<mnode> m) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(m->inum_, "initialize_dir");
  load_dir(i, m);
}

void mfs_interface::metadata_op_start(size_t cpu, u64 tsc_val) {
  metadata_log->update_start_tsc(cpu, tsc_val);
}

void mfs_interface::metadata_op_end(size_t cpu, u64 tsc_val) {
  metadata_log->update_end_tsc(cpu, tsc_val);
}

// Adds a metadata operation to the logical log.
void mfs_interface::add_to_metadata_log(mfs_operation *op) {
  metadata_log->add_operation(op);
}

// Applies all metadata operations logged in the logical log. Called on sync.
void mfs_interface::process_metadata_log() {
  mfs_operation_vec ops;
  u64 sync_tsc = 0;
  if (cpuid::features().rdtscp)
    sync_tsc = rdtscp();
  else
    sync_tsc = rdtsc_serialized();
  {
    auto guard = metadata_log->wait_synchronize(sync_tsc);
    for (auto it = metadata_log->operation_vec.begin(); it !=
      metadata_log->operation_vec.end(); it++)
      ops.push_back(*it);
    metadata_log->operation_vec.clear();
  }

  for (auto it = ops.begin(); it != ops.end(); it++) {
    transaction *tr = new transaction((*it)->timestamp);
    (*it)->apply(tr);
    add_to_journal(tr);
    delete (*it);
  }

}

void
mfs_interface::sync_dirty_files()
{
  superblock sb;

  get_superblock(&sb);

  for (int i = 0; i < sb.ninodes; i++) {
    sref<mnode> m;
    if (inum_to_mnode->lookup(i, &m) && m) {
      if(m->type() == mnode::types::file)
        m->as_file()->sync_file();
    }
  }
}

void
mfs_interface::evict_bufcache()
{
  superblock sb;

  get_superblock(&sb);

  for (int i = 0; i < sb.ninodes; i++) {
    sref<mnode> m;

    if (inum_to_mnode->lookup(i, &m) && m) {
      if(m->type() == mnode::types::file) {
        sref<inode> ip = get_inode(m->inum_, "evict_bufcache");
        drop_bufcache(ip);
      }
    }
  }
}

void
mfs_interface::evict_pagecache()
{
  superblock sb;

  get_superblock(&sb);

  for (int i = 0; i < sb.ninodes; i++) {
    sref<mnode> m;

    if (inum_to_mnode->lookup(i, &m) && m) {
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

// Usage: To evict the (clean) blocks cached in the buffer-cache, do:
// $ echo 1 > /dev/evict_caches
static int
evict_caches(mdev*, const char *buf, u32 n)
{
  cprintf("evict_caches: dropping buffer-cache blocks\n");

  if (n != 1) {
    cprintf("evict_caches: invalid number of characters (%d)\n", n);
    return n;
  }

  if (*buf == '1')
    rootfs_interface->evict_bufcache();
  else
    cprintf("evict_caches: invalid option %c\n", *buf);

  return n;
}

// Applies metadata operations logged in the logical journal. Called on
// fsync to resolve any metadata dependencies.
void mfs_interface::process_metadata_log(u64 max_tsc, u64 inum, bool isdir) {
  mfs_operation_vec dependent_ops;
  {
    // Synchronize the oplog loggers.
    auto guard = metadata_log->wait_synchronize(max_tsc);
    // Find out the metadata operations the fsync() call depends on and just
    // apply those. inum refers to the mnode that is executing the fsync().
    find_dependent_ops(inum, dependent_ops, isdir);
  }

  if (dependent_ops.size() == 0)
    return;

  auto it = dependent_ops.end();
  do {
    it--;
    transaction *tr = new transaction((*it)->timestamp);
    (*it)->apply(tr);
    add_to_journal(tr);
    delete (*it);
  } while (it != dependent_ops.begin());

}

// Goes through the metadata log and filters out the operations that the fsync()
// call depends on. inum refers to the mnode that is executing the fsync().
void mfs_interface::find_dependent_ops(u64 inum,
  mfs_operation_vec &dependent_ops, bool isdir) {
 
  if (metadata_log->operation_vec.size() == 0)
    return;

  // mnode_vec is a monotonically growing list of mnode inums whose dependent
  // operations need to be flushed too.
  std::vector<u64> mnode_vec;
  mnode_vec.push_back(inum);
  auto it = metadata_log->operation_vec.end();
  int index = metadata_log->operation_vec.size();
  do {
    it--;
    index--;
    if (isdir && (*it)->check_parent_dependency(mnode_vec, inum)) {
      dependent_ops.push_back(*it);
      metadata_log->operation_vec.erase(metadata_log->operation_vec.begin()+index);
    } else if((*it)->check_dependency(mnode_vec)) {
      dependent_ops.push_back(*it);
      metadata_log->operation_vec.erase(metadata_log->operation_vec.begin()+index);
    }
  } while (it != metadata_log->operation_vec.begin());
}

// Create operation
void mfs_interface::mfs_create(mfs_operation_create *op, transaction *tr) {
  scoped_gc_epoch e;
  if (op->mnode_type == mnode::types::file)      // sync the parent directory too
    create_file_if_new(op->mnode, op->parent, op->mnode_type, op->name, tr, true);     
  else if (op->mnode_type == mnode::types::dir)  
    create_dir_if_new(op->mnode, op->parent, op->mnode_type, op->name, tr, true);
}

// Link operation
void mfs_interface::mfs_link(mfs_operation_link *op, transaction *tr) {
  scoped_gc_epoch e;
  create_directory_entry(op->parent, op->name, op->mnode, op->mnode_type, tr);
  update_dir_inode(op->parent, tr);
}

// Unlink operation
void mfs_interface::mfs_unlink(mfs_operation_unlink *op, transaction *tr) {
  scoped_gc_epoch e;
  char str[DIRSIZ];
  strcpy(str, op->name);
  unlink_old_inode(op->parent, str, tr);
  update_dir_inode(op->parent, tr);
}

// Rename operation
void mfs_interface::mfs_rename(mfs_operation_rename *op, transaction *tr) {
  scoped_gc_epoch e;
  char str[DIRSIZ];
  strcpy(str, op->name);

  create_directory_entry(op->new_parent, op->newname, op->mnode, op->mnode_type, tr);
  update_dir_inode(op->new_parent, tr);

  unlink_old_inode(op->parent, str, tr);
  update_dir_inode(op->parent, tr);
}

// Logs a transaction to the physical journal. Does not apply it to the disk yet
void mfs_interface::add_to_journal(transaction *tr) {
  fs_journal->add_transaction(tr);
}

// Commit a transaction to the journal and apply the committed changes to
// the original locations on the disk filesystem.
// Locking rule: The caller needs to hold the journal's write_lock.
void
mfs_interface::__flush_transaction(transaction *tr)
{
  // Update the free bitmap on the disk.
  balloc_on_disk(tr->allocated_block_list, tr);
  bfree_on_disk(tr->free_block_list, tr);

  tr->prepare_for_commit();

  // Write out the transaction blocks to the disk journal in timestamp order.
  write_transaction_to_journal(tr->blocks, tr->timestamp_);

  // Now that the transaction has been committed, mark the freed blocks as
  // free in the in-memory free-bit-vector.
  for (auto f = tr->free_block_list.begin();
       f != tr->free_block_list.end(); f++)
    free_block(*f);

  // This transaction has been committed to the journal. Writeback the changes
  // to the original locations on the disk.
  for (auto b = tr->blocks.begin(); b != tr->blocks.end(); b++)
    (*b)->writeback();

  // The blocks have been written to disk successfully. Safe to delete
  // this transaction from the journal. (This means that all the
  // transactions till this point have made it to the disk. So the journal
  // can simply be truncated.) Since the journal is static, the journal file
  // simply needs to be zero-filled.)
  clear_journal();
}

// Logs a transaction in the disk journal and then applies it to the disk
void
mfs_interface::add_fsync_to_journal(transaction *tr)
{
  auto journal_lock = fs_journal->prepare_for_commit();

  __flush_transaction(tr);
}

// Writes out the physical journal to the disk, and applies the committed
// transactions to the disk filesystem.
void
mfs_interface::flush_journal()
{
  auto journal_lock = fs_journal->prepare_for_commit();

  for (auto it = fs_journal->transaction_log.begin();
       it != fs_journal->transaction_log.end(); it++) {

    __flush_transaction(*it);

    delete (*it);
  }

  fs_journal->transaction_log.clear();
}

// Writes out a single journal transaction to disk
void mfs_interface::write_transaction_to_journal(
    const std::vector<std::unique_ptr<transaction_diskblock> >& vec, const u64 timestamp) {
  transaction *trans = new transaction(0);
  assert(sv6_journal);
  u32 offset = fs_journal->current_offset();

  // Each transaction begins with a start block
  journal_block_header hdstart(timestamp, 0, jrnl_start);
  char buf[sizeof(journal_block_header)];
  memset(buf, 0, sizeof(buf)); 
  memmove(buf, (void*)&hdstart, sizeof(hdstart));
  char databuf[BSIZE];
  memset(databuf, 0, sizeof(databuf));

  if (writei(sv6_journal, buf, offset, sizeof(buf), trans) != sizeof(buf))
    panic("Journal write failed");
  offset += sizeof(buf);
  if (writei(sv6_journal, databuf, offset, BSIZE, trans) != BSIZE)
    panic("Journal write failed");
  offset += BSIZE;

  // Write out the transaction diskblocks
  for (auto it = vec.begin(); it != vec.end(); it++) {
    journal_block_header hd(timestamp, (*it)->blocknum, jrnl_data);
    memset(buf, 0, sizeof(buf));
    memmove(buf, (void*)&hd, sizeof(hd));
    if (writei(sv6_journal, buf, offset, sizeof(buf), trans) != sizeof(buf))
      panic("Journal write failed");
    offset += sizeof(buf);
    if (writei(sv6_journal, (*it)->blockdata, offset, BSIZE, trans) != BSIZE)
      panic("Journal write failed");
    offset += BSIZE;
  }

  // Each transaction ends with a commit block
  journal_block_header hdcommit(timestamp, 0, jrnl_commit);
  memset(buf, 0, sizeof(buf)); 
  memmove(buf, (void*)&hdcommit, sizeof(hdcommit));
  memset(databuf, 0, sizeof(databuf));
  if (writei(sv6_journal, buf, offset, sizeof(buf), trans) != sizeof(buf))
    panic("Journal write failed");
  offset += sizeof(buf);
  if (writei(sv6_journal, databuf, offset, BSIZE, trans) != BSIZE)
    panic("Journal write failed");
  offset += BSIZE;

  // Update the journal file inode too.
  fs_journal->update_offset(offset);
  trans->write_to_disk();
  delete trans;
}

// Called on reboot after a crash. Applies committed transactions.
void mfs_interface::process_journal() {
  u32 offset = 0;
  u64 current_transaction = 0;
  bool jrnl_error = false;
  transaction *trans = new transaction(0);
  std::vector<std::unique_ptr<transaction_diskblock> > block_vec;
  sv6_journal = namei(sref<inode>(), "/sv6journal");
  assert(sv6_journal);
  ilock(sv6_journal, 1);

  while (!jrnl_error) {
    char hdbuf[sizeof(journal_block_header)];
    char databuf[BSIZE];
    if (readi(sv6_journal, hdbuf, offset, sizeof(journal_block_header)) !=
      sizeof(journal_block_header))
      break;

    char hdcmp[sizeof(journal_block_header)];
    memset(&hdcmp, 0, sizeof(journal_block_header));
    if (!memcmp(hdcmp, hdbuf, sizeof(journal_block_header)))
      break;  // Zero-filled block indicates end of journal

    offset += sizeof(journal_block_header);
    if (readi(sv6_journal, databuf, offset, BSIZE) != BSIZE)
      break;
    offset += BSIZE;

    journal_block_header hd;
    memset(&hd, 0, sizeof(hd));
    memmove(&hd, hdbuf, sizeof(hdbuf));

    switch (hd.block_type) {
      case jrnl_start:
        current_transaction = hd.timestamp;
        block_vec.clear();
        break;
      case jrnl_data:
        if (hd.timestamp == current_transaction) {
          block_vec.push_back(std::make_unique<transaction_diskblock>(hd.blocknum,databuf));
        }
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
void mfs_interface::clear_journal() {
  assert(sv6_journal);
  ilock(sv6_journal, 1);
  zero_fill(sv6_journal, fs_journal->current_offset());
  iunlock(sv6_journal);
  fs_journal->update_offset(0);
}

bool mfs_interface::inode_lookup(u64 mnode, u64 *inum) {
  if (!mnode_to_inode)
    panic("mnode_to_inode mapping does not exist yet");
  if (mnode_to_inode->lookup(mnode, inum))
    return true;
  return false;
}

void mfs_interface::create_mapping(u64 mnode, u64 inode) {
  if (!mnode_to_inode)
    panic("mnode_to_inode mapping does not exist yet");
  mnode_to_inode->insert(mnode, inode);
}

sref<mnode> mfs_interface::mnode_alloc(u64 inum, u8 mtype) {
  auto m = root_fs->alloc(mtype);
  inum_to_mnode->insert(inum, m.mn());
  create_mapping(m.mn()->inum_, inum);
  return m.mn();
}

sref<mnode> mfs_interface::load_dir_entry(u64 inum, sref<mnode> parent) {
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
    mlinkref ilink(parent);
    ilink.acquire();
    m->as_dir()->insert(parent_name, &ilink);
  }

  return m;
}

void mfs_interface::load_dir(sref<inode> i, sref<mnode> m) {
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

    mlinkref ilink(mf);
    ilink.acquire();
    m->as_dir()->insert(name, &ilink);
  }
}

sref<mnode> mfs_interface::load_root() {
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
void mfs_interface::initialize_free_bit_vector() {
  sref<buf> bp;
  int b, bi, free_bit_count;
  u32 blocknum;
  superblock sb;
  get_superblock(&sb);

  for(b = 0; b < sb.size; b += BPB) {
    blocknum = BBLOCK(b, sb.ninodes);
    bp = buf::get(1, blocknum);
    auto copy = bp->read();

    // Allocate the memory for free_bit_vector in one shot, instead
    // of doing it piecemeal using .emplace_back() in a loop.
    free_bit_count = min(BPB, sb.size - b);
    free_bit_vector.reserve(free_bit_count);

    for(bi = 0; bi < free_bit_count; bi++) {
      int m = 1 << (bi % 8);
      bool f = ((copy->data[bi/8] & m) == 0) ? true : false;
      free_bit_vector.emplace_back(f);
    }
  }
}

// Return the block number of a free block in the free_bit_vector.
u32 mfs_interface::alloc_block() {
  u32 index = 0;
  for (auto it = free_bit_vector.begin(); it != free_bit_vector.end(); it++) {
    if (it->is_free) {
      auto lock = it->write_lock.guard();
      //Re-confirm that the block is indeed free, with the lock held.
      if (it->is_free) {
        it->is_free = false;
        break;
      }
    }
    index++;
  }
  return index;
}

// Mark a block as free in the free_bit_vector.
void mfs_interface::free_block(u32 bno) {
  if (free_bit_vector.at(bno).is_free)
    panic("freeing free block");
  auto lock = free_bit_vector.at(bno).write_lock.guard();
  free_bit_vector.at(bno).is_free = true;
}

void mfs_interface::print_free_blocks(print_stream *s)
{
  u32 count = 0;

  for (auto it = free_bit_vector.begin(); it != free_bit_vector.end(); it++) {
    if (it->is_free) {
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

void kfreeblockprint(print_stream *s)
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

void initfs() {
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

  root_inum = rootfs_interface->load_root()->inum_;
  /* the root inode gets an extra reference because of its own ".." */
}
