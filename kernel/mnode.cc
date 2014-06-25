#include "types.h"
#include "kernel.hh"
#include "mnode.hh"
#include "weakcache.hh"
#include "atomic_util.hh"
#include "percpu.hh"
#include "vm.hh"

namespace {
  // 32MB icache (XXX make this proportional to physical RAM)
  weakcache<pair<mfs*, u64>, mnode> mnode_cache(32 << 20);
};

sref<mnode>
mfs::get(u64 inum)
{
  for (;;) {
    sref<mnode> m = mnode_cache.lookup(make_pair(this, inum));
    if (m) {
      // wait for the mnode to be loaded from disk
      while (!m->valid_) {
        /* spin */
      }
      return m;
    }

    panic("read in from disk not implemented");
  }
}

mlinkref
mfs::alloc(u8 type, u64 parent)
{
  scoped_cli cli;
  auto inum = mnode::inumber(type, myid(), (*next_inum_)++).v_;

  sref<mnode> m;
  switch (type) {
  case mnode::types::dir:
    m = sref<mnode>::transfer(new mdir(this, inum, parent));
    break;

  case mnode::types::file:
    m = sref<mnode>::transfer(new mfile(this, inum, parent));
    break;

  case mnode::types::dev:
    m = sref<mnode>::transfer(new mdev(this, inum));
    break;

  case mnode::types::sock:
    m = sref<mnode>::transfer(new msock(this, inum));
    break;

  default:
    panic("unknown type in inum 0x%lx", inum);
  }

  if (!mnode_cache.insert(make_pair(this, inum), m.get()))
    panic("mnode_cache insert failed (duplicate inumber?)");

  m->cache_pin(true);
  m->valid_ = true;
  mlinkref mlink(std::move(m));
  mlink.transfer();
  return mlink;
}

mnode::mnode(mfs* fs, u64 inum)
  : fs_(fs), inum_(inum), initialized_(false), cache_pin_(false), dirty_(false),
    valid_(false)
{
  kstats::inc(&kstats::mnode_alloc);
}

void
mnode::cache_pin(bool flag)
{
  if (cache_pin_ == flag || !cmpxch(&cache_pin_, !flag, flag))
    return;

  if (flag)
    inc();
  else
    dec();
}

void
mnode::dirty(bool flag)
{
  if (dirty_ == flag)
    return;
  cmpxch(&dirty_, !flag, flag);
}

bool
mnode::is_dirty()
{
  return dirty_;
}

void
mnode::onzero()
{
  mnode_cache.cleanup(weakref_);
  kstats::inc(&kstats::mnode_free);
  delete this;
}

void
mnode::linkcount::onzero()
{
  /*
   * This might fire several times, because the link count of a zero-nlink
   * parent directory can be temporarily revived by mkdir (see create).
   */
  mnode* m = container_from_member(this, &mnode::nlink_);
  m->cache_pin(false);
}

void
mfile::resizer::resize_nogrow(u64 newsize)
{
  u64 oldsize = mf_->size_;
  mf_->size_ = newsize;
  assert(PGROUNDUP(newsize) <= PGROUNDUP(oldsize));
  auto begin = mf_->pages_.find(PGROUNDUP(newsize) / PGSIZE);
  auto end = mf_->pages_.find(PGROUNDUP(oldsize) / PGSIZE);
  auto lock = mf_->pages_.acquire(begin, end);
  mf_->pages_.unset(begin, end);

  if (PGROUNDDOWN(newsize) > PGROUNDDOWN(oldsize)) {
    /* Grew to a multiple of PGSIZE */
    mf_->pages_.find(oldsize / PGSIZE)->set_partial_page(false);
  }

  if (PGROUNDDOWN(newsize) < PGROUNDDOWN(oldsize) && PGOFFSET(newsize)) {
    /* Shrunk, and last page is partial */
    mf_->pages_.find(newsize / PGSIZE)->set_partial_page(true);
  }
}

void
mfile::resizer::resize_append(u64 size, sref<page_info> pi)
{
  assert(PGROUNDUP(mf_->size_) / PGSIZE + 1 == PGROUNDUP(size) / PGSIZE);

  if (PGOFFSET(mf_->size_)) {
    /* Also filled out last partial page */
    mf_->pages_.find(mf_->size_ / PGSIZE)->set_partial_page(false);
  }

  auto it = mf_->pages_.find(PGROUNDUP(mf_->size_) / PGSIZE);
  // XXX This is rather unfortunate for the first write to a file
  // since the fill will expand the lock to a huge range.  This would
  // be a great place to use lock_for_fill if we had it.
  auto lock = mf_->pages_.acquire(it);
  page_state ps(pi);
  if (PGOFFSET(size))
    ps.set_partial_page(true);
  ps.set_dirty_bit(true);
  mf_->pages_.fill(it, ps);
  mf_->size_ = size;
}

void
mfile::ondisk_size(u64 size)
{
  size_ = size;
}

mfile::page_state
mfile::get_page(u64 pageidx)
{
  auto it = pages_.find(pageidx);
  if (!it.is_set()) {
    if (!initialized_ && fs_ == root_fs) {
      cprintf("Should not be happening. File not initialized from disk yet!\n");
      initialized_ = true;
      rootfs_interface->initialize_file(root_fs->get(inum_));
      barrier();
    }
    if (pageidx < PGROUNDUP(size_) / PGSIZE && fs_ == root_fs) {
      // Read page from disk
      char *p = zalloc("file page");
      assert(p);

      auto pi = sref<page_info>::transfer(new (page_info::of(p)) page_info());
      size_t pos = pageidx * PGSIZE;
      size_t nbytes = size_ - pos;
      if (nbytes > PGSIZE)
        nbytes = PGSIZE;

      auto lock = pages_.acquire(it);
      assert(nbytes == rootfs_interface->load_file_page(inum_, p, pos, nbytes));
      page_state ps(pi);
      if (PGOFFSET(nbytes))
        ps.set_partial_page(true);
      pages_.fill(it, ps);
    }

    else
      return mfile::page_state();
  }

  return it->copy_consistent();
}

void
mfile::dirty_page(u64 pageidx)
{
  auto it = pages_.find(pageidx);
  assert(it.is_set());
  it->set_dirty_bit(true);
}

// This function gets called when a file is truncated. Page table mappings for
// any pages that are no longer a part of the file need to be cleared from vmaps
// that have the file mmapped. Each page_info object keeps track of these vmaps
// via an oplog-maintained reverse map. This rmap is now traversed to unmap the
// truncated pages from the vmaps in question.
void
mfile::remove_pgtable_mappings(u64 start_offset) {
  auto page_trunc_start = pages_.find(PGROUNDUP(start_offset) / PGSIZE);
  for (auto it = page_trunc_start; it != pages_.end(); ) {
    // Skip unset spans
    if (!it.is_set()) {
      it += it.base_span();
      continue;
    }
    auto pg_info = it->get_page_info();
    if (pg_info) {
      std::vector<page_info::rmap_entry> rmap_vec;
      pg_info->get_rmap_vector(rmap_vec);
      for (auto rmap_it = rmap_vec.begin(); rmap_it != rmap_vec.end(); rmap_it++)
        rmap_it->first->remove_mapping(rmap_it->second);
    }
    ++it;
  }
}

void
mfile::sync_file()
{
  if (!is_dirty())
    return;

  // Apply pending metadata operations to the disk filesystem first.
  // This takes care of any dependencies.
  rootfs_interface->process_metadata_log();

  transaction *trans = new transaction();
 
  u64 ilen = rootfs_interface->get_file_size(inum_);
  auto size = read_size();
  // If the in-memory file is shorter, truncate the file on the disk.
  if (ilen > *size)
    rootfs_interface->truncate_file(inum_, *size, trans);

  // Flush all in-memory file pages to disk.
  auto page_end = pages_.find(PGROUNDUP(*size) / PGSIZE);
  auto lock = pages_.acquire(pages_.begin(), page_end);
  for (auto it = pages_.begin(); it != page_end; ) {
    // Skip unset spans
    if (!it.is_set()) {
      it += it.base_span();
      continue;
    }
    if (!it->is_dirty_page()) {
      ++it;
      continue;
    }

    size_t pos = it.index() * PGSIZE;
    size_t nbytes = *size - pos;
    if (nbytes > PGSIZE)
      nbytes = PGSIZE;
    assert(nbytes == rootfs_interface->sync_file_page(inum_, 
                    (char*)it->get_page_info()->va(), pos, nbytes, trans));
    it->set_dirty_bit(false);
    ++it;
  }
  rootfs_interface->update_file_size(inum_, *size, trans);
  
  // Add the fsync transaction to the journal and flush the journal to disk.
  rootfs_interface->add_to_journal(trans);
  rootfs_interface->flush_journal();
  dirty(false);
}

void
mdir::sync_dir()
{
  if (!is_dirty())
    return;

  // Apply pending metadata operations to the disk filesystem first.
  // This takes care of any dependencies.
  rootfs_interface->process_metadata_log();

  // Flush out the physical journal to disk. The directory entries do not need
  // to be flushed explicitly. If there were any operations on the directory
  // they will have been applied when the logical log was processed. This means
  // that the fsync will not block any operations on the mdir.
  rootfs_interface->flush_journal();
  dirty(false);
}

void
mfsprint(print_stream *s)
{
  auto stats = mnode_cache.get_stats();
  s->println("mnode cache:");
  s->println("  ", stats.items, " items");
  s->println("  ", stats.used_buckets, " used / ",
             stats.total_buckets, " total buckets (",
             stats.used_buckets * 100 / stats.total_buckets, "%)");
  s->println("  ", stats.max_chain, " max chain length");
  s->println("  ", stats.items / stats.total_buckets, " avg chain length");
  if (stats.used_buckets)
    s->println("  ", stats.items / stats.used_buckets, " avg used chain length");
}
