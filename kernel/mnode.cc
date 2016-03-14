#include "types.h"
#include "kernel.hh"
#include "mnode.hh"
#include "weakcache.hh"
#include "atomic_util.hh"
#include "percpu.hh"
#include "vm.hh"

namespace {
  // 32MB mcache (XXX make this proportional to physical RAM)
  weakcache<pair<mfs*, u64>, mnode> mnode_cache(32 << 20);
};

sref<mnode>
mfs::mget(u64 mnum)
{
  for (;;) {
    sref<mnode> m = mnode_cache.lookup(make_pair(this, mnum));
    if (m) {
      // Wait for the mnode to be ready.
      while (!m->valid_) {
        /* spin */
      }
      return m;
    }

  return sref<mnode>();
  }
}

mlinkref
mfs::alloc(u8 type, u64 parent_mnum)
{
  scoped_cli cli;
  auto mnum = mnode::mnumber(type, myid(), (*next_mnum_)++).v_;

  sref<mnode> m;
  switch (type) {
  case mnode::types::dir:
    m = sref<mnode>::transfer(new mdir(this, mnum, parent_mnum));
    break;

  case mnode::types::file:
    m = sref<mnode>::transfer(new mfile(this, mnum, parent_mnum));
    break;

  case mnode::types::dev:
    m = sref<mnode>::transfer(new mdev(this, mnum));
    break;

  case mnode::types::sock:
    m = sref<mnode>::transfer(new msock(this, mnum));
    break;

  default:
    panic("unknown type in mnum 0x%lx", mnum);
  }

  if (!mnode_cache.insert(make_pair(this, mnum), m.get()))
    panic("mnode_cache insert failed (duplicate mnumber?)");

  rootfs_interface->alloc_mnode_lock(mnum);
  rootfs_interface->alloc_metadata_log(mnum);
  m->cache_pin(true);
  m->valid_ = true;
  mlinkref mlink(std::move(m));
  mlink.transfer();
  return mlink;
}

mnode::mnode(mfs* fs, u64 mnum)
  : fs_(fs), mnum_(mnum), initialized_(false), cache_pin_(false), dirty_(false),
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
  // The mnode's refcount dropping to zero is a reliable indication that it
  // is now safe to delete the inode and its file-contents (i.e., the link
  // count is zero and no process has this file open). So add the corresponding
  // delete operation to the MFS log.

  // TODO: If the kernel crashes at this point (i.e., after the last unlink
  // operation has been logged/applied, but before the corresponding delete
  // operation has been logged), then we won't be able to reclaim the blocks
  // belonging to this file upon reboot.

  // Always log all delete operations in a common, dedicated (per-cpu) mfs_log
  // instance, using the special invalid mnode number MFS_DELETE_MNUM (which is
  // guaranteed to never clash with any valid mnode number).

  rootfs_interface->metadata_op_start(MFS_DELETE_MNUM, myid(), get_tsc());

  if (type() == types::file)
    this->as_file()->remove_pgtable_mappings(0);

  mfs_operation *op = new mfs_operation_delete(rootfs_interface, get_tsc(), mnum_);
  rootfs_interface->add_to_metadata_log(MFS_DELETE_MNUM, op);
  rootfs_interface->metadata_op_end(MFS_DELETE_MNUM, myid(), get_tsc());

  rootfs_interface->free_metadata_log(mnum_);
  rootfs_interface->free_mnode_lock(mnum_);

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
  mf_->dirty(true);
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
  mf_->dirty(true);
}

void
mfile::set_page_dirty(u64 pageidx)
{
  auto it = pages_.find(pageidx);
  auto lock = pages_.acquire(it);
  it->set_dirty_bit(true);
}

void
mfile::resizer::initialize_from_disk(u64 size)
{
  auto begin = mf_->pages_.begin();
  auto end = mf_->pages_.find(PGROUNDUP(size) / PGSIZE);
  auto lock = mf_->pages_.acquire(begin, end);
  page_state ps(true);
  mf_->pages_.fill(begin, end, ps);
  mf_->size_ = size;
}

mfile::page_state
mfile::get_page(u64 pageidx)
{
  auto it = pages_.find(pageidx);
  if (!it.is_set())
    return mfile::page_state();
  if (it->get_page_info() == nullptr && fs_ == root_fs) {
      // We may block.  If scheduling is disabled, this could lead to
      // deadlock, so throw a blocking_io exception with an IO retry.
      // Currently this is used by pagefault and may need to be
      // generalized to be used in other situations.
      if (check_critical(critical_mask::NO_SCHED))
        throw blocking_io(sref<mfile>::newref(this), pageidx);

      // Read page from disk
      char *p = zalloc("file page");
      assert(p);

      auto pi = sref<page_info>::transfer(new (page_info::of(p)) page_info());
      size_t pos = pageidx * PGSIZE;
      size_t nbytes = size_ - pos;
      if (nbytes > PGSIZE)
        nbytes = PGSIZE;

      size_t bytes_read = rootfs_interface->load_file_page(mnum_, p, pos, nbytes);
      assert(nbytes == bytes_read);
      auto lock = pages_.acquire(it);
      page_state ps(pi);
      if (PGOFFSET(nbytes))
        ps.set_partial_page(true);
      pages_.fill(it, ps);
  }

  return it->copy_consistent();
}

// Evict a (clean) page from the page-cache.
void
mfile::put_page(u64 pageidx)
{
  auto it = pages_.find(pageidx);
  if (!it.is_set())
    return;

  sref<page_info> pi = it->get_page_info();
  if (pi != nullptr && fs_ == root_fs) {
    // Don't evict dirty pages.
    if (it->is_dirty_page())
      return;

    it->reset_page_info();

    std::vector<page_info::rmap_entry> rmap_vec;
    pi->get_rmap_vector(rmap_vec);
    for (auto rmap_it = rmap_vec.begin(); rmap_it != rmap_vec.end(); rmap_it++)
      rmap_it->first->clear_mapping(rmap_it->second);

    pi->dec();

  }
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
        rmap_it->first->delete_mapping(rmap_it->second);
    }
    ++it;
  }
}

// Drop the (clean) page-cache pages associated with this file.
void
mfile::drop_pagecache()
{
  u64 mlen = *read_size();
  auto page_end = pages_.find(PGROUNDUP(mlen) / PGSIZE);
  for (auto it = pages_.begin(); it != page_end; ) {
    // Skip unset spans
    if (!it.is_set()) {
      mlen = *read_size();
      if (mlen <= it.index()*PGSIZE)
        break;
      it += it.base_span();
      if (mlen <= it.index()*PGSIZE)
        break;
      continue;
    }

    // Don't evict dirty pages.
    if (it->is_dirty_page()) {
      ++it;
      continue;
    }

    put_page(it.index());
    ++it;
  }
}

// If flush_journal is set to true, the transaction added to the journal is
// immediately flushed to the disk. (The caller might choose to set this to
// false in order to do a single flush covering many transactions, later on).
void
mfile::sync_file(bool flush_journal)
{
  if (!is_dirty())
    return;

  auto lock = fsync_lock_.guard();

  transaction *trans = new transaction();
  u64 mlen = *read_size();

  // Flush all in-memory file pages to disk.
  auto page_end = pages_.find(PGROUNDUP(mlen) / PGSIZE);
  for (auto it = pages_.begin(); it != page_end; ) {
    // Skip unset spans
    if (!it.is_set()) {
      mlen = *read_size();
      if (mlen <= it.index()*PGSIZE)
        break;
      it += it.base_span();
      if (mlen <= it.index()*PGSIZE)
        break;
      continue;
    }

    if (!it->is_dirty_page()) {
      ++it;
      continue;
    }

    size_t pos = it.index() * PGSIZE;

    // The actual number of bytes to be written is mlen - pos, but we use
    // PGSIZE as the size argument in order to avoid expensive Read-Modify-Writes
    // in writei() [because synchronous reads kill the performance benefits of
    // asynchronous writes]. Since the rest of the bytes in the page are
    // zero anyway, this is harmless; we won't leak any random bytes into the
    // file.
    assert(PGSIZE == rootfs_interface->sync_file_page(mnum_,
                    (char*)it->get_page_info()->va(), pos, PGSIZE, trans));
    it->set_dirty_bit(false);
    ++it;
  }

  u64 ilen = rootfs_interface->get_file_size(mnum_);
  // If the in-memory file is shorter, truncate the file on the disk.
  if (ilen > mlen)
    rootfs_interface->truncate_file(mnum_, mlen, trans);

  // Update the size and the inode.
  rootfs_interface->update_file_size(mnum_, mlen, trans);

  // Add the fsync transaction to the journal and possibly flush the journal
  // to disk.
  rootfs_interface->add_fsync_to_journal(trans, flush_journal);

  // We are done with this transaction, if we already flushed it to disk.
  if (flush_journal)
    delete trans;

  dirty(false);
}

void
mdir::sync_dir()
{
  if (!is_dirty())
    return;

  // In the fsync path, the caller file_inode::fsync() invokes
  // process_metadata_log_and_flush() with the appropriate parameters before
  // invoking sync_dir(), and that is sufficient to sync directories.
  // See the comment in file_inode::fsync() for more details.

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
