#include "types.h"
#include "kernel.hh"
#include "fs.h"
#include "file.hh"
#include "mnode.hh"
#include "linearhash.hh"
#include "mfs.hh"

static linearhash<u64, sref<mnode>> *inum_to_mnode;

/*static sref<mnode> load_inum_recurse(u64 inum);

static void
load_dir_recurse(sref<inode> i, sref<mnode> m)
{
  dirent de;
  for (size_t pos = 0; pos < i->size; pos += sizeof(de)) {
    assert(sizeof(de) == readi(i, (char*) &de, pos, sizeof(de)));
    if (!de.inum)
      continue;

    sref<mnode> mf = load_inum_recurse(de.inum);
    strbuf<DIRSIZ> name(de.name);
    if (name == ".")
      continue;

    mlinkref ilink(mf);
    ilink.acquire();
    m->as_dir()->insert(name, &ilink);
  }
}

static void
load_file(sref<inode> i, sref<mnode> m)
{
  for (size_t pos = 0; pos < i->size; pos += PGSIZE) {
    char* p = zalloc("load_file");
    assert(p);

    auto pi = sref<page_info>::transfer(new (page_info::of(p)) page_info());

    size_t nbytes = i->size - pos;
    if (nbytes > PGSIZE)
      nbytes = PGSIZE;

    assert(nbytes == readi(i, p, pos, nbytes));
    auto resize = m->as_file()->write_size();
    resize.resize_append(pos + nbytes, pi);
  }
}*/

void
initialize_file(sref<mnode> m)
{
  scoped_gc_epoch e;
  u64 inum = 0;
  sref<inode> i;

  if (!mnode_to_inode)
    panic("mnode_to_inode mapping does not exist yet");
  if (!mnode_to_inode->lookup(m->inum_, &inum)) //XXX What if the mapping does not exist?
    panic("Mapping for mnode# %ld does not exist", m->inum_);
  i = iget(1, inum);
  if(!i)
    panic("LOAD_FILE: inode %ld does not exist", inum);

  m->as_file()->ondisk_size(i->size);
}

int
load_file_page(sref<mnode> m, char *p, size_t pos, size_t nbytes)
{
  scoped_gc_epoch e;
  u64 inum = 0;
  sref<inode> i;
  if (!mnode_to_inode)
    panic("mnode_to_inode mapping does not exist yet");
  if (!mnode_to_inode->lookup(m->inum_, &inum)) //XXX What if the mapping does not exist?
    panic("Mapping for mnode# %ld does not exist", m->inum_);
  i = iget(1, inum);
  if(!i)
    panic("LOAD_FILE_PAGE: inode %ld does not exist", inum);
  return readi(i, p, pos, nbytes);
}

static void
create_mapping(u64 mnode, u64 inode)
{
  if (!mnode_to_inode)
    panic("mnode_to_inode mapping does not exist yet");
  mnode_to_inode->insert(mnode, inode);
}

static sref<mnode>
mnode_alloc(u64 inum, u8 mtype)
{
  auto m = root_fs->alloc(mtype);
  inum_to_mnode->insert(inum, m.mn());
  create_mapping(m.mn()->inum_, inum);
  return m.mn();
}

/*static sref<mnode>
load_inum_recurse(u64 inum)
{
  sref<mnode> m;
  if (inum_to_mnode->lookup(inum, &m))
    return m;

  sref<inode> i = iget(1, inum);
  switch (i->type.load()) {
  case T_DIR:
    m = mnode_alloc(inum, mnode::types::dir);
    load_dir_recurse(i, m);
    break;

  case T_FILE:
    m = mnode_alloc(inum, mnode::types::file);
    load_file(i, m);
    break;

  default:
    panic("unhandled inode %ld type %d\n", inum, i->type.load());
  }

  return m;
}*/

static sref<mnode>
load_dir_entry(u64 inum)
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
    cprintf("unhandled inode type %d\n", i->type.load());
  }
  return m;
}

static void
load_dir(sref<inode> i, sref<mnode> m)
{
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

void
initialize_dir(sref<mnode> m)
{
  scoped_gc_epoch e;
  u64 inum = 0;
  sref<inode> i;

  if (!mnode_to_inode)
    panic("mnode_to_inode mapping does not exist yet");
  if (!mnode_to_inode->lookup(m->inum_, &inum)) //XXX What if the mapping does not exist?
    panic("Mapping for mnode# %ld does not exist", m->inum_);
  i = iget(1, inum);
  if(!i)
    panic("LOAD_DIR: inode %ld does not exist", inum);
  
  load_dir(i, m);
}

static sref<mnode>
load_root()
{
  sref<mnode> m;
  if (inum_to_mnode->lookup(1, &m))
    return m;

  sref<inode> i = iget(1, 1);
  assert(i->type.load() == T_DIR);
  m = mnode_alloc(1, mnode::types::dir);
  return m;
}

void
mfsload()
{
  root_fs = new mfs();
  anon_fs = new mfs();

  inum_to_mnode = new linearhash<u64, sref<mnode>>(4099);
  mnode_to_inode = new linearhash<u64, u64>(4099);
  root_inum = load_root()->inum_;
  /* the root inode gets an extra reference because of its own ".." */
  //delete inum_to_mnode;
}
