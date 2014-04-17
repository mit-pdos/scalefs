#include "types.h"
#include "kernel.hh"
#include "fs.h"
#include "file.hh"
#include "mnode.hh"
#include "mfs.hh"
#include "scalefs.hh"

mfs_interface::mfs_interface() {
  inum_to_mnode = new linearhash<u64, sref<mnode>>(4099);
  mnode_to_inode = new linearhash<u64, u64>(4099);
  fs_journal = new journal();
}

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

void mfs_interface::initialize_file(sref<mnode> m) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(m->inum_, "initialize_file");
  if(i)
    m->as_file()->ondisk_size(i->size);
}

int mfs_interface::load_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "load_file_page");
  if(i)
    return readi(i, p, pos, nbytes);
  return -1;
}

u64 mfs_interface::get_file_size(u64 mfile_inum) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "get_file_size");
  if(i)
    return i->size;
  return 0;
}

void mfs_interface::update_file_size(u64 mfile_inum, u32 size, transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "get_file_size");
  if(i)
    update_size(i, size, tr); 
}

int mfs_interface::sync_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes,
    transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "sync_file_page");
  if(i)
    return writei(i, p, pos, nbytes, tr);
  return -1;
}

u64 mfs_interface::create_file_if_new(u64 mfile_inum, u64 parent, u8 type,
  char *name, transaction *tr, bool sync_parent) {
  u64 inum = 0, parent_inum = 0, returnval = 0;
  if (inode_lookup(mfile_inum, &inum))
    return 0;
  if (!inode_lookup(parent, &parent_inum))
    panic("create_file_if_new: parent %ld does not exist\n", parent);
    // XXX what if the parent needs to be synced too
    
  sref<inode> i;
  i = ialloc(1, type);
  mnode_to_inode->insert(mfile_inum, i->inum);
  inum_to_mnode->insert(i->inum, root_fs->get(mfile_inum));
  returnval = i->inum;
  iupdate(i, tr);
  tr->log_new_file(i->inum);
  iunlock(i);

  if (sync_parent) {
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

void mfs_interface::truncate_file(u64 mfile_inum, u32 offset, transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mfile_inum, "truncate_file");
  if(i)
    itrunc(i, offset, tr);
}

u64 mfs_interface::create_dir_if_new(u64 mdir_inum, u64 parent, u8 type,
  char *name, transaction *tr, bool sync_parent) {
  u64 inum = 0, parent_inum = 0, returnval = 0;
  if (inode_lookup(mdir_inum, &inum))
    return 0;
  if (!inode_lookup(parent, &parent_inum))
    panic("create_dir_if_new: parent %ld does not exist\n", parent);
  // XXX what if the parent needs to be synced too

  sref<inode> i, parenti;
  i = ialloc(1, type);
  mnode_to_inode->insert(mdir_inum, i->inum);
  inum_to_mnode->insert(i->inum, root_fs->get(mdir_inum));
  returnval = i->inum;
  dirlink(i, "..", parent_inum, true);
  dir_flush(i, tr);
  iunlock(i);

  if (sync_parent) {
    parenti = iget(1, parent_inum);
    ilock(parenti, 1);
    dirlink(parenti, name, i->inum, true);
    dir_flush(parenti, tr);
    iunlock(parenti);
  }

  return returnval;
}

void mfs_interface::create_directory_entry(u64 mdir_inum, char *name, u64
    dirent_inum, u8 type, transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mdir_inum, "allocate_inode_for_dirent");
  if(!i)
    panic("allocate_inode_for_dirent: directory %ld does not exist\n", mdir_inum);

  sref<inode> di = dirlookup(i, name);
  if (di) 
    return;   // directory entry exists. XXX Check if the inum has changed

  u64 inum = 0;
  inode_lookup(dirent_inum, &inum);
  if (inum) { // inode exists. Just create a dir entry. No need to allocate
    dirlink(i, name, inum, (type == mnode::types::dir)?true:false);
  } else {  // allocate new inode
    // XXX create_if_new also updates the new inode that was allocated. Is this
    // the correct design decision here? Probably yes.
    if (type == mnode::types::file) {
      inum = create_file_if_new(dirent_inum, mdir_inum, type, name, tr);
      dirlink(i, name, inum, false);
    } else if (type == mnode::types::dir) {
      inum = create_dir_if_new(dirent_inum, mdir_inum, type, name, tr, false);
      dirlink(i, name, inum, true);
    }
  } 
}

void mfs_interface::unlink_old_inodes(u64 mdir_inum, std::vector<char*> names_vec,
    transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mdir_inum, "unlink_old_inodes");
  if(!i)
    panic("unlink_old_inodes: directory %ld does not exist\n", mdir_inum);

  dir_remove_entries(i, names_vec, tr);
  update_dir(i, tr);
}

void mfs_interface::update_dir_inode(u64 mdir_inum, transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mdir_inum, "update_dir_inode");
  if(i)
    update_dir(i, tr);
}

void mfs_interface::initialize_dir(sref<mnode> m) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(m->inum_, "initialize_dir");
  if(i)
    load_dir(i, m);
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

sref<mnode> mfs_interface::load_dir_entry(u64 inum) {
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
    cprintf("unhandled inode type %d\n", i->type.load());
  }
  return m;
}

void mfs_interface::load_dir(sref<inode> i, sref<mnode> m) {
  dirent de;
  for (size_t pos = 0; pos < i->size; pos += sizeof(de)) {
    assert(sizeof(de) == readi(i, (char*) &de, pos, sizeof(de)));
    if (!de.inum)
      continue;

    sref<mnode> mf = load_dir_entry(de.inum);
    strbuf<DIRSIZ> name(de.name);
    if (name == ".")
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

void mfsload() {
  root_fs = new mfs();
  anon_fs = new mfs();
  rootfs_interface = new mfs_interface();
  root_inum = rootfs_interface->load_root()->inum_;
  // XXX Check for orphan inodes and free up disk space
  /* the root inode gets an extra reference because of its own ".." */
}
