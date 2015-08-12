#pragma once

/*
 * A bucket-chaining hash table.
 */

#include "spinlock.hh"
#include "seqlock.hh"
#include "lockwrap.hh"
#include "hash.hh"
#include "ilist.hh"
#include "hpet.hh"
#include "cpuid.hh"

template<class K, class V>
class chainhash {
private:
  struct item : public rcu_freed {
    item(const K& k, const V& v)
      : rcu_freed("chainhash::item", this, sizeof(*this)),
        key(k), val(v) {}
    void do_gc() override { delete this; }
    NEW_DELETE_OPS(item);

    islink<item> link;
    seqcount<u32> seq;
    const K key;
    V val;
  };

  struct bucket {
    spinlock lock __mpalign__;
    islist<item, &item::link> chain;

    ~bucket() {
      while (!chain.empty()) {
        item *i = &chain.front();
        chain.pop_front();
        gc_delayed(i);
      }
    }
  };

  u64 nbuckets_;
  bool dead_;
  bucket* buckets_;

public:
  chainhash(u64 nbuckets) : nbuckets_(nbuckets), dead_(false) {
    buckets_ = new bucket[nbuckets_];
    assert(buckets_);
  }

  ~chainhash() {
    delete[] buckets_;
  }

  NEW_DELETE_OPS(chainhash);

  bool insert(const K& k, const V& v, u64 *tsc = NULL) {
    if (dead_ || lookup(k))
      return false;

    bucket* b = &buckets_[hash(k) % nbuckets_];
    scoped_acquire l(&b->lock);

    if (dead_)
      return false;

    for (const item& i: b->chain)
      if (i.key == k)
        return false;

    b->chain.push_front(new item(k, v));
    if (tsc)
      *tsc = get_tsc();
    return true;
  }

  bool remove(const K& k, const V& v, u64 *tsc = NULL) {
    if (!lookup(k))
      return false;

    bucket* b = &buckets_[hash(k) % nbuckets_];
    scoped_acquire l(&b->lock);

    auto i = b->chain.before_begin();
    auto end = b->chain.end();
    for (;;) {
      auto prev = i;
      ++i;
      if (i == end)
        return false;
      if (i->key == k && i->val == v) {
        b->chain.erase_after(prev);
        gc_delayed(&*i);
        if (tsc)
          *tsc = get_tsc();
        return true;
      }
    }
  }

  bool remove(const K& k, u64 *tsc = NULL) {
    if (!lookup(k))
      return false;

    bucket* b = &buckets_[hash(k) % nbuckets_];
    scoped_acquire l(&b->lock);

    auto i = b->chain.before_begin();
    auto end = b->chain.end();
    for (;;) {
      auto prev = i;
      ++i;
      if (i == end)
        return false;
      if (i->key == k) {
        b->chain.erase_after(prev);
        gc_delayed(&*i);
        if (tsc)
          *tsc = get_tsc();
        return true;
      }
    }
  }

  bool replace_from(const K& kdst, const V* vpdst,
                    chainhash* src, const K& ksrc,
                    const V& vsrc, u64 *tsc = NULL)
  {
    /*
     * A special API used by rename.  Atomically performs the following
     * steps, returning false if any of the checks fail:
     *
     *  - if vpdst!=nullptr, checks this[kdst]==*vpdst
     *  - if vpdst==nullptr, checks this[kdst] is not set
     *  - checks src[ksrc]==vsrc
     *  - removes src[ksrc]
     *  - sets this[kdst] = vsrc
     */
    bucket* bdst = &buckets_[hash(kdst) % nbuckets_];
    bucket* bsrc = &src->buckets_[hash(ksrc) % src->nbuckets_];

    scoped_acquire lsrc, ldst;
    if (bsrc == bdst) {
      lsrc = bsrc->lock.guard();
    } else if (bsrc < bdst) {
      lsrc = bsrc->lock.guard();
      ldst = bdst->lock.guard();
    } else {
      ldst = bdst->lock.guard();
      lsrc = bsrc->lock.guard();
    }

    auto srci = bsrc->chain.before_begin();
    auto srcend = bsrc->chain.end();
    auto srcprev = srci;
    for (;;) {
      ++srci;
      if (srci == srcend)
        return false;
      if (srci->key != ksrc) {
        srcprev = srci;
        continue;
      }
      if (srci->val != vsrc)
        return false;
      break;
    }

    for (item& i: bdst->chain) {
      if (i.key == kdst) {
        if (vpdst == nullptr || i.val != *vpdst)
          return false;
        auto w = i.seq.write_begin(); 
        i.val = vsrc;
        bsrc->chain.erase_after(srcprev);
        gc_delayed(&*srci);
        if (tsc)
          *tsc = get_tsc();
        return true;
      }
    }

    if (vpdst != nullptr)
      return false;

    bsrc->chain.erase_after(srcprev);
    gc_delayed(&*srci);
    bdst->chain.push_front(new item(kdst, vsrc));
    if (tsc)
      *tsc = get_tsc();
    return true;
  }

  bool replace_common_mnode(const K& kdst, const V* vpdst,
                    chainhash* src, const K& ksrc, u64 *tsc = NULL)
  {
    // This is used by the rename syscall when the source and destination file
    // names point to the same mnode. The destination is read under a seqlock
    // and the source is simply removed. This ensures no cache line movement for
    // the destination if the optimistic read succeeds. The call does not
    // succeed (and returns false) if the destination file name does not exist
    // anymore, or if the mnode it maps to has changed in any way. In this case
    // the rename syscall retries with a conventional replace_from() call.
    bucket* bdst = &buckets_[hash(kdst) % nbuckets_];
    auto dsti = bdst->chain.before_begin();
    auto dstend = bdst->chain.end();
    for (;;) {
      dsti++;
      // Fail if the destination file name does not exist anymore.
      if (dsti == dstend)
        return false;
      if (dsti->key != kdst)
        continue;
      break;
    }

    V dstv;
    auto r = dsti->seq.read_begin();
    do {
      dstv = dsti->val;
    } while (r.do_retry());
    // Fail if the mnode for the destination file name has changed.
    if (dstv != *vpdst)
      return false;

    // Delete the source optimistically
    if (!src->remove(ksrc, *vpdst, tsc))
      return false;
    // If the mnode for the destination file name has changed, revert the
    // deletion of the source and return false.
    if (r.need_retry()) {
      // We should be able to insert the source that we just deleted. If we
      // don't succeed we'll end up losing the file name.
      assert(src->insert(ksrc, *vpdst));
      return false;
    }
    return true;
  }

  bool enumerate(const K* prev, K* out) const {
    scoped_gc_epoch rcu_read;

    bool prevbucket = (prev != nullptr);
    for (u64 i = prev ? hash(*prev) % nbuckets_ : 0; i < nbuckets_; i++) {
      bucket* b = &buckets_[i];
      bool found = false;
      for (const item& i: b->chain) {
        if ((!prevbucket || *prev < i.key) && (!found || i.key < *out)) {
          *out = i.key;
          found = true;
        }
      }
      if (found)
        return true;
      prevbucket = false;
    }

    return false;
  }

  template<class CB>
  void enumerate(CB cb) const {
    scoped_gc_epoch rcu_read;

    for (u64 i = 0; i < nbuckets_; i++) {
      bucket* b = &buckets_[i];

      for (const item& i: b->chain) {
        V val = *seq_reader<V>(&i.val, &i.seq);
        if (cb(i.key, val))
          return;
      }
    }
  }

  bool lookup(const K& k, V* vptr = nullptr) const {
    scoped_gc_epoch rcu_read;

    bucket* b = &buckets_[hash(k) % nbuckets_];
    for (const item& i: b->chain) {
      if (i.key != k)
        continue;
      if (vptr)
        *vptr = *seq_reader<V>(&i.val, &i.seq);
      return true;
    }
    return false;
  }

  bool remove_and_kill(const K& k, const V& v) {
    if (dead_)
      return false;

    for (u64 i = 0; i < nbuckets_; i++)
      for (const item& ii: buckets_[i].chain)
        if (ii.key != k || ii.val != v)
          return false;

    for (u64 i = 0; i < nbuckets_; i++)
      buckets_[i].lock.acquire();

    bool killed = !dead_;
    for (u64 i = 0; i < nbuckets_; i++)
      for (const item& ii: buckets_[i].chain)
        if (ii.key != k || ii.val != v)
          killed = false;

    if (killed) {
      dead_ = true;
      bucket* b = &buckets_[hash(k) % nbuckets_];
      item* i = &b->chain.front();
      assert(i->key == k && i->val == v);
      b->chain.pop_front();
      gc_delayed(i);
    }

    for (u64 i = 0; i < nbuckets_; i++)
      buckets_[i].lock.release();

    return killed;
  }

  bool killed() const {
    return dead_;
  }
};
