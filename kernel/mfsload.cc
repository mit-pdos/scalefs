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

int mfs_interface::load_file_page(u64 mnode_inum, char *p, size_t pos, size_t nbytes) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mnode_inum, "load_file_page");
  if(i)
    return readi(i, p, pos, nbytes);
  return -1;
}

u64 mfs_interface::get_file_size(u64 mnode_inum) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mnode_inum, "get_file_size");
  if(i)
    return i->size;
  return 0;
}

int mfs_interface::sync_file_page(u64 mnode_inum, char *p, size_t pos, size_t nbytes,
    transaction *tr) {
  scoped_gc_epoch e;
  sref<inode> i = get_inode(mnode_inum, "sync_file_page");
  if(i)
    return writei(i, p, pos, nbytes, tr);
  return -1;
}

void mfs_interface::create_file_if_new(u64 mnode_inum, u8 type, transaction *tr) {
  u64 inum = 0;
  if (inode_lookup(mnode_inum, &inum))
    return;
  sref<inode> i;
  i = ialloc(1, type);
  mnode_to_inode->insert(mnode_inum, i->inum);
  inum_to_mnode->insert(i->inum, root_fs->get(mnode_inum));
  iupdate(i, tr);
  iunlock(i);
  //XXX record creation of new file under parent dir
}

void mfs_interface::truncate_file(u64 mnode_inum, u32 offset, transaction *tr) {
  sref<inode> i = get_inode(mnode_inum, "truncate_file");
  if(i)
    itrunc(i, offset, tr);
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
  /* the root inode gets an extra reference because of its own ".." */
}
