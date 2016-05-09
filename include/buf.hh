#pragma once

#include "refcache.hh"
#include "seqlock.hh"
#include "sleeplock.hh"
#include "fs.h"
#include "atomic_util.hh"
#include "lockwrap.hh"
#include "weakcache.hh"
#include "disk.hh"

class buf : public refcache::weak_referenced {
public:
  struct bufdata {
    char data[BSIZE];
  };

  typedef pair<u32, u64> key_t;

  static bool in_bufcache(u32 dev, u64 block);
  static sref<buf> get(u32 dev, u64 block, bool skip_disk_read = false);
  static void put(u32 dev, u64 block);
  void writeback(bool sync = true);
  void writeback_async();
  void add_to_transaction(transaction *trans);
  void add_blocknum_to_transaction(transaction *trans);

  void async_iowait_init() {
    //inc();
    dc_ = make_sref<disk_completion>();
  }

  void async_iowait() {
    dc_->wait();
    dc_.reset();
    //dec();
  }

  u32 dev() { return dev_; }
  u64 block() { return block_; }
  bool dirty() { return dirty_; }

  void cache_pin(bool flag) {
    if (flag)
      inc();
    else
      dec();
  }

  seq_reader<bufdata> read() {
    return seq_reader<bufdata>(data_, &seq_);
  }

  class buf_dirty {
  public:
    buf_dirty(buf* b) : b_(b) { if (b_) b_->mark_dirty(); }
    ~buf_dirty() { }
    buf_dirty(buf_dirty&& o) : b_(o.b_) { o.b_ = nullptr; }
    void operator=(buf_dirty&& o) { b_ = o.b_; o.b_ = nullptr; }

  private:
    buf* b_;
  };

  class buf_writer : public ptr_wrap<bufdata>,
                     public lock_guard<sleeplock>,
                     public seq_writer,
                     public buf_dirty {
  public:
    buf_writer(bufdata* d, sleeplock* l, seqcount<u32>* s, buf* b)
      : ptr_wrap<bufdata>(d), lock_guard<sleeplock>(l),
        seq_writer(s), buf_dirty(b) {}
  };

  buf_writer write() {
    return buf_writer(data_, &write_lock_, &seq_, this);
  }

  // Same as write(), except that the block is not marked dirty.
  // Used to get exclusive (i.e., write) access to the block without
  // disturbing the dirty flag or the reference count.
  buf_writer write_clean() {
    return buf_writer(data_, &write_lock_, &seq_, nullptr);
  }

private:
  const u32 dev_;
  const u64 block_;

  seqcount<u32> seq_;
  sleeplock write_lock_;
  sleeplock writeback_lock_;
  std::atomic<bool> dirty_;
  sref<disk_completion> dc_;

  bufdata *data_;

  buf(u32 dev, u64 block)
    : dev_(dev), block_(block), dirty_(false)
  {
    data_ = (bufdata *) kmalloc(sizeof(bufdata), "bufdata");
  }
  void onzero() override;
  NEW_DELETE_OPS(buf);

  ~buf()
  {
    kmfree(data_, sizeof(bufdata));
  }

  void mark_dirty() {
    if (cmpxch(&dirty_, false, true))
      inc();
  }

  void mark_clean() {
    if (cmpxch(&dirty_, true, false))
      dec();
  }
};

template<>
inline u64
hash(const buf::key_t& k)
{
  return k.first ^ k.second;
}
