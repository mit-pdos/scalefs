#pragma once

#include "bitset.hh"
#include "cpu.hh"
#include "spinlock.hh"
#include "percpu.hh"
#include "hpet.hh"
#include "cpuid.hh"
#include "sleeplock.hh"
#include "lockwrap.hh"

#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>
#include <algorithm>
#include <queue>

// OpLog is a technique for scaling objects that are frequently
// written and rarely read.  It works by logging modification
// operations to per-CPU logs and only applying these modification
// operations when a read needs to observe the object's state.
namespace oplog {
  enum {
    CACHE_SLOTS = 4096
  };

  // A base class for objects whose modification operations are logged
  // and synchronized to the object's state only when the state needs
  // to be observed.
  //
  // Classes wishing to apply OpLog should implement a "logger class"
  // and subclass @c logged_object.  Methods that modify the object's
  // state should call @c get_logger to get an instance of the logger
  // class and should call a method of the logger class to log the
  // operation.  Methods that read the object's state should call @c
  // synchronize to apply all outstanding logged operations before
  // they observe the object's state.
  //
  // @c logged_object takes care of making this memory-efficient:
  // rather than simply keeping per-CPU logs for every object, it
  // maintains a fixed size cache of logs per CPU so that only
  // recently modified objects are likely to have logs.
  //
  // @tparam Logger A class that logs operations to be applied to the
  // object later.  This is the type returned by get_logger.  There
  // may be many Logger instances created per logged_object.  Logger
  // must have a default constructor, but there are no other
  // requirements.
  template<typename Logger>
  class logged_object
  {
  public:
    constexpr logged_object(bool use_sleeplock) : sync_spinlock_(),
      sync_sleeplock_(), use_sleeplock_(use_sleeplock) { }

    // logged_object is meant to be subclassed, so it needs a virtual
    // destructor.
    virtual ~logged_object() { }

    // A Logger instance protected by a lock.  Users of this class
    // should not attempt to hold a reference to the protected logger
    // longer than the locked_logger object remains live.
    class locked_logger
    {
      lock_guard<spinlock> lock_;
      Logger *logger_;

    public:
      locked_logger(lock_guard<spinlock> &&lock, Logger *logger)
        : lock_(std::move(lock)), logger_(logger) { }

      locked_logger(locked_logger &&o)
        : lock_(std::move(o.lock_)), logger_(o.logger_)
      {
        o.logger_ = nullptr;
      }

      locked_logger &operator=(locked_logger &&o)
      {
        lock_ = std::move(o.lock_);
        logger_ = o.logger_;
        o.logger_ = nullptr;
      }

      // Return the protected Logger instance.  Note that there is no
      // operator*, since that would encourage decoupling the life
      // time of the locked_logger from the lifetime of the Logger*.
      Logger *operator->() const
      {
        return logger_;
      }
    };

  protected:
    // Return a locked operation logger for this object on the specified
    // cpu. In general, this logger will be CPU-local, meaning that
    // operations from different cores can be performed in parallel and
    // without communication.
    locked_logger get_logger(int cpu)
    {
      auto id = cpu;
      auto my_way = cache_[id].hash_way(this);
    back_out:
      auto guard = my_way->lock_.guard();
      auto cur_obj = my_way->obj_.load(std::memory_order_relaxed);

      if (cur_obj != this) {
        if (cur_obj) {
          // Evict this logger.  In the unlikely event of a race
          // between this and synchronize, we may deadlock here if we
          // simply acquire cur_obj's sync lock.  Hence, we perform
          // deadlock avoidance.
	  // (Furthermore, since the sync lock can be a sleeplock, while
	  // way->lock_ is a spinlock, we can't actually afford to sleep on
	  // contention here; if we did, it would lead to "sleeping inside
	  // atomic section" bug).
          lock_guard<spinlock> sync_spin_guard;
          lock_guard<sleeplock> sync_sleep_guard;

          if (cur_obj->use_sleeplock_) {
            sync_sleep_guard = cur_obj->sync_sleeplock_.try_guard();
            if (!sync_sleep_guard)
              // We would deadlock with synchronize; back out.
              goto back_out;
          } else {
            sync_spin_guard = cur_obj->sync_spinlock_.try_guard();
            if (!sync_spin_guard)
              // We would deadlock with synchronize; back out.
              goto back_out;
          }

          // XXX Since we don't do a full synchronize here, we lose
          // some of the potential memory overhead benefits of the
          // logger cache for ordered loggers like tsc_logged_object.
          // These have to keep around all operations anyway until
          // someone calls synchronize.  We could keep track of this
          // object in the locked_logger and call synchronize when it
          // gets released.
          cur_obj->flush_logger(&my_way->logger_);
          cur_obj->cpus_.atomic_reset(id);
        }
        // Put this object in this way's tag
        my_way->obj_.store(this, std::memory_order_relaxed);
      } else {
        assert(cpus_[id]);
      }

      if (!cpus_[id])
        cpus_.atomic_set(id);
      return locked_logger(std::move(guard), &my_way->logger_);
    }

    // This is a helper function; do not call it directly. Use
    // synchronize_with_spinlock() or synchronize_with_sleeplock() instead.
    void __synchronize()
    {
      // Repeatedly gather loggers until we see that the CPU set is
      // empty.  We can't check the whole CPU set atomically, but
      // that's okay.  Since we hold the sync lock, only we can clear
      // bits in the CPU set, so while operations may happen between
      // when we observe that CPU 0 is not in the set and when we
      // observe that CPU n is not in the set, *if* we observe that
      // all of the bits are zero, *then* we had a consistent snapshot
      // as of when we observed that CPU 0's bit was zero.
      while (1) {
        bool any = false;
        // Gather loggers
        for (auto cpu : cpus_) {
          // XXX Is the optimizer smart enough to lift the hash
          // computation?
          auto way = cache_[cpu].hash_way(this);
          auto way_guard = way->lock_.guard();
          auto cur_obj = way->obj_.load(std::memory_order_relaxed);
          assert(cur_obj == this);
          flush_logger(&way->logger_);
          cpus_.atomic_reset(cpu);
          way->obj_.store(nullptr, std::memory_order_relaxed);
          any = true;
        }
        if (!any)
          break;
        // Make sure we see concurrent updates to cpus_.
        barrier();
      }

      // Tell the logged object that it has a consistent set of
      // loggers and should do any final flushing.
      flush_finish();
    }

    // Acquire a per-object lock, apply all logged operations to this
    // object, and return the per-object lock.  The caller may keep
    // this lock live for as long as it needs to prevent modifications
    // to the object's synchronized value. The caller has the choice
    // of using a spinlock or a sleeplock for synchronization.
    lock_guard<spinlock> synchronize_with_spinlock()
    {
      auto guard = sync_spinlock_.guard();
      __synchronize();
      return std::move(guard);
    }

    // See comment above synchronize_with_spinlock().
    lock_guard<sleeplock> synchronize_with_sleeplock()
    {
      auto guard = sync_sleeplock_.guard();
      __synchronize();
      return std::move(guard);
    }

    // Flush one logger, resetting it to its initial state.  This may
    // update the object's state, but is not required to (for some
    // loggers, this may be impossible when there are other loggers
    // still cached).  This is called with a locks that prevent
    // concurrent flush_* calls and that prevent l from being returned
    // by get_logger.
    virtual void flush_logger(Logger *l) = 0;

    // Perform final synchronization of the object's state.  This is
    // called by synchronize after it has flushed a consistent
    // snapshot of loggers for this object.  This is called with locks
    // that prevents concurrent flush_* calls.
    virtual void flush_finish() = 0;

  private:
    struct way
    {
      std::atomic<logged_object*> obj_;
      spinlock lock_;
      Logger logger_;
    };

    struct cache
    {
      way ways_[CACHE_SLOTS];

      way *hash_way(logged_object *obj) const
      {
        // Hash based on Java's HashMap re-hashing function.
        uint64_t wayno = (uintptr_t)obj;
        wayno ^= (wayno >> 32) ^ (wayno >> 20) ^ (wayno >> 12);
        wayno ^= (wayno >> 7) ^ (wayno >> 4);
        wayno %= CACHE_SLOTS;
        return const_cast<way *>(&ways_[wayno]);
        //return &ways_[wayno];
      }
    };

  protected:
    // Per-type, per-CPU, per-object logger.  The per-CPU part of this
    // is unprotected because we lock internally.
    static percpu<cache, NO_CRITICAL> cache_;

    // Bitmask of CPUs that have logged operations for this object.
    // Bits can be set without any lock, but can only be cleared when
    // holding sync_lock_.
    bitset<NCPU> cpus_;

    // This lock serializes log flushes and protects clearing cpus_.
    // Note: sync_sleeplock_ is a sleeplock, whereas way->lock_ is a spinlock.
    // So the only legal lock ordering is to nest the way->lock_ inside the
    // sync_sleeplock_. However, we are forced to acquire these locks in the
    // opposite order in get_logger(); but luckily, our deadlock avoidance
    // scheme retries if the sync_sleeplock_ is contended, so we never actually
    // sleep while holding the spinlock, which makes it safe.
    spinlock sync_spinlock_;
    sleeplock sync_sleeplock_;
    bool use_sleeplock_;
  };

  template<typename Logger>
  percpu<typename logged_object<Logger>::cache, NO_CRITICAL> logged_object<Logger>::cache_; 

  // The logger class used by tsc_logged_object.
  class tsc_logger
  {
  public:
    class op
    {
    public:
      NEW_DELETE_OPS(op);
      const uint64_t tsc;
      op(uint64_t tsc) : tsc(tsc) { }
      virtual ~op() { }
      virtual void run() = 0;
      virtual void print() = 0;
    };

  private:
    template<class CB>
    class op_inst : public op
    {
      CB cb_;
    public:
      NEW_DELETE_OPS(op_inst);
      op_inst(uint64_t tsc, CB &&cb) : op(tsc), cb_(cb) { }
      ~op_inst() { }

      void run() override
      {
        cb_();
      }
      void print() override
      {
        cb_.print();
      }
    };

    // Logged operations in TSC order
    std::vector<std::unique_ptr<op> > ops_;
    typedef decltype(ops_)::iterator op_iter;

    void reset()
    {
      ops_.clear();
    }

    friend class tsc_logged_object;
    friend class mfs_logged_object;

  public:
    tsc_logger() = default;
    tsc_logger(tsc_logger &&o) = default;
    tsc_logger &operator=(tsc_logger &&o) = default;

    // Log the operation cb, which must be a callable.  cb will be
    // called with no arguments when the logs need to be
    // synchronized.
    template<typename CB>
    void push(CB &&cb)
    {
      // We use rdtscp because all instructions before it must
      // retire before it reads the time stamp, which means we must
      // get a time stamp after the lock acquisition in get_logger.
      // rdtscp does not prevent later instructions from issuing
      // before it, but that's okay up to the lock release.  The
      // lock release will not move before the TSC read because we
      // have to write the value of the TSC to memory, which
      // introduces a data dependency from the rdtscp to this write,
      // and the lock release also writes to memory, which
      // introduces a TSO dependency from the TSC memory write to
      // the lock release.
      ops_.push_back(std::make_unique<op_inst<CB> >(
                       get_tsc(), std::forward<CB>(cb)));
    }

    // Same as push<CB>, the only difference being that the tsc value is passed
    // here instead of calling rdtscp() to get a tsc value. This is used to log
    // filesystem operations in the logical log, where the tsc is read off at
    // the linearization point of the operation (when applied on mfs).
    template<typename CB>
    void push_with_tsc(CB &&cb)
    {
      ops_.push_back(std::make_unique<op_inst<CB> >(
                       cb.get_timestamp(), std::forward<CB>(cb)));
    }

    static bool compare_tsc(const std::unique_ptr<op> &op1, const std::unique_ptr<op> &op2) {
      return (op1->tsc < op2->tsc);
    }

    void sort_ops() {
      std::sort(ops_.begin(), ops_.end(), compare_tsc);
    }
    
    void print_ops() {
      for (auto it = ops_.begin(); it != ops_.end(); it++)
        (*it)->print();
    }

    // Returns an iterator 'it' where all operations in [ops_.begin(),
    // it) have timestamps less than or equal to max_tsc.
    op_iter ops_before_max_tsc(u64 max_tsc) {
      auto it = ops_.begin(), end = ops_.end();
      for (; it != end; it++)
        if ((*it)->tsc > max_tsc)
          break;
      return it;
    }
  };

  // A logger that applies operations in global timestamp order using
  // synchronized TSCs.
  class tsc_logged_object : public logged_object<tsc_logger>
  {
  public:
    tsc_logged_object(bool use_sleeplock) : logged_object(use_sleeplock),
      use_sleeplock_(use_sleeplock) {}
  protected:
    std::vector<tsc_logger> pending_;
    bool use_sleeplock_;

    void clear_loggers()
    {
      lock_guard<spinlock> spin_guard;
      lock_guard<sleeplock> sleep_guard;

      if (use_sleeplock_)
        sleep_guard = sync_sleeplock_.guard();
      else
        spin_guard = sync_spinlock_.guard();

      while (1) {
        bool any = false;
        // Gather loggers
        for (auto cpu : cpus_) {
          // XXX Is the optimizer smart enough to lift the hash
          // computation?
          auto way = cache_[cpu].hash_way(this);
          auto way_guard = way->lock_.guard();
          auto cur_obj = way->obj_.load(std::memory_order_relaxed);
          assert(cur_obj == this);
          way->logger_.reset();
          cpus_.atomic_reset(cpu);
          way->obj_.store(nullptr, std::memory_order_relaxed);
          any = true;
        }
        if (!any)
          break;
        // Make sure we see concurrent updates to cpus_.
        barrier();
      }
    }

    void flush_logger(tsc_logger *l) override
    {
      pending_.emplace_back(std::move(*l));
      l->reset();
    }

    void print_pending_loggers() {
      for (auto it = pending_.begin(); it != pending_.end(); it++)
        it->print_ops();
    }

    static std::vector<size_t> seq_vector(size_t x)
    {
      std::vector<size_t> vec;
      for (size_t i = 0; i < x; ++i)
        vec.push_back(i);
      return vec;
    }

    // This should heap-merge all of the loggers
    // in pending_ and apply their operations in order.
    void flush_finish() override {
      if (pending_.empty())
        return;

      struct pos { tsc_logger::op_iter next, end; };
      std::vector<pos> posns;
      std::vector<std::unique_ptr<tsc_logger::op> > merged_ops;
      for(auto &logger : pending_) {
        if (logger.ops_.empty())
          continue;
        logger.sort_ops();  //XXX(rasha) Are the inidividual loggers already in tsc order?
        posns.push_back({logger.ops_.begin(), logger.ops_.end()});
      }
      if (posns.empty())
        return;

      // Merge the operations using a heap of indices into posns
      auto compare = [&](size_t a, size_t b) -> bool {
        return (*posns[a].next)->tsc > (*posns[b].next)->tsc;
      };
      std::priority_queue<size_t, std::vector<size_t>, decltype(compare)> heap(
        compare, seq_vector(posns.size()));
      while (!heap.empty()) {
        auto top = heap.top();
        merged_ops.push_back(std::move(*posns[top].next));
        ++posns[top].next;
        heap.pop();
        if (posns[top].next != posns[top].end)
          heap.push(top);
      }
      assert(std::is_sorted(merged_ops.begin(), merged_ops.end(),
                            tsc_logger::compare_tsc));
 
      for(auto &op : merged_ops)
        op->run();
      for(auto &logger : pending_)
        logger.reset();
      pending_.clear();
    }

  public:
  ~tsc_logged_object() {
    clear_loggers();
  }

  };

  class mfs_logged_object : public tsc_logged_object {
  public:
    mfs_logged_object(bool use_sleeplock) : tsc_logged_object(use_sleeplock),
                                            synced_upto_tsc(0) {}

  private:
    struct mfs_tsc {
      u64 tsc_value;
      seqcount<u32> seq;
      mfs_tsc() { tsc_value = 0; }
    };

    // The starting time of the latest mfs metadata operation on each core
    percpu<mfs_tsc> mfs_start_tsc;
    // The ending time of the latest mfs metadata operation on each core
    percpu<mfs_tsc> mfs_end_tsc;
    // Lock to protect writes to both mfs_start_tsc and mfs_end_tsc.
    percpu<sleeplock> mfs_tsc_lock;

    // synced_upto_tsc tracks the latest tsc value that the user has invoked
    // synchronized_upto_tsc() on.
    u64 synced_upto_tsc;

    // Heap-merges pending loggers and applies the operations, leaving behind
    // operations that have timestamps greater than max_tsc.
    void flush_finish_max_timestamp(u64 max_tsc) {
      if (pending_.empty())
        return;

      struct pos {
        tsc_logger::op_iter next, end;
        tsc_logger *logger;
      };
      std::vector<pos> posns;
      std::vector<std::unique_ptr<tsc_logger::op> > merged_ops;
      for(auto &logger : pending_) {
        logger.sort_ops();
        auto end = logger.ops_before_max_tsc(max_tsc);
        if (logger.ops_.begin() == end)
          continue;
        posns.push_back({logger.ops_.begin(), end, &logger});
      }
      if (posns.empty())
        return;

      // Merge the operations using a heap of indices into posns
      auto compare = [&](size_t a, size_t b) -> bool {
        return (*posns[a].next)->tsc > (*posns[b].next)->tsc;
      };
      std::priority_queue<size_t, std::vector<size_t>, decltype(compare)> heap(
        compare, seq_vector(posns.size()));
      while (!heap.empty()) {
        auto top = heap.top();
        merged_ops.push_back(std::move(*posns[top].next));
        ++posns[top].next;
        heap.pop();
        if (posns[top].next != posns[top].end)
          heap.push(top);
      }
      assert(std::is_sorted(merged_ops.begin(), merged_ops.end(),
                            tsc_logger::compare_tsc));

      assert(merged_ops.front()->tsc >= synced_upto_tsc);

      for(auto &op : merged_ops)
        op->run();
      for(auto &pos : posns)
        pos.logger->ops_.erase(pos.logger->ops_.begin(), pos.end);

      // Remove empty loggers from pending
      auto dst = pending_.begin();
      for(auto src = dst, end = pending_.end(); src != end; ++src) {
        if(!src->ops_.empty()) {
          if(dst != src)
            *dst = std::move(*src);
          ++dst;
        }
      }
      pending_.erase(dst, pending_.end());
    }

  public:

    lock_guard<sleeplock> get_tsc_lock_guard(int cpu) {
      auto guard = mfs_tsc_lock[cpu].guard();
      return std::move(guard);
    }

    // The caller must hold mfs_tsc_lock[cpu], before calling update_start_tsc()
    // and update_end_tsc(); Further, both these functions must be invoked in
    // the same critical section, without releasing the lock.

    void update_start_tsc(size_t cpu, u64 start_tsc) {
      auto w = mfs_start_tsc[cpu].seq.write_begin();
      mfs_start_tsc[cpu].tsc_value = start_tsc;
    }

    void update_end_tsc(size_t cpu, u64 end_tsc) {
      auto w = mfs_end_tsc[cpu].seq.write_begin();
      mfs_end_tsc[cpu].tsc_value = end_tsc;
    }

    // The same as logged_object::synchronize except that we might have to wait
    // for cores which have in-flight operations that need to be logged before
    // synchronization.
    //
    // synchronize_upto_tsc(): Applies all logged operations whose timestamps
    // are less than or equal to 'max_tsc'.
    //
    // This is only ever called from the ScaleFS code, so we directly use a
    // sleeplock here for synchronization, and don't provide a spinlock
    // alternative.
    lock_guard<spinlock> synchronize_upto_tsc(u64 max_tsc) {

      // Avoid repeated work if we already synchronized upto the given timestamp.
      if (max_tsc <= synced_upto_tsc)
        return std::move(sync_spinlock_.guard());

      for (int i = 0; i < NCPU; i++) {
        // end_tsc <= start_tsc indicates that the core in question is executing
        // an operation that might not have been logged yet. We can only be sure
        // that the operation has been logged once the end_tsc value has been
        // updated, which is the last thing an operation does before exiting. We
        // need to wait for an operation that is executing to be logged in order
        // to know where the linearization point of the operation lies with
        // respect to max_tsc.

        u64 start_tsc, end_tsc;
        start_tsc = *seq_reader<u64>(&mfs_start_tsc[i].tsc_value,
                                     &mfs_start_tsc[i].seq);

        do {
          end_tsc = *seq_reader<u64>(&mfs_end_tsc[i].tsc_value,
                                     &mfs_end_tsc[i].seq);
        } while (start_tsc && start_tsc <= max_tsc && end_tsc <= start_tsc);

        assert(!start_tsc || start_tsc > max_tsc || end_tsc > start_tsc);
      }

      auto guard = sync_spinlock_.guard();
      while (1) {
        bool any = false;
        // Gather loggers
        for (auto cpu : cpus_) {
          auto way = cache_[cpu].hash_way(this);
          auto way_guard = way->lock_.guard();
          auto cur_obj = way->obj_.load(std::memory_order_relaxed);
          assert(cur_obj == this);
          // Flush only those operations whose linearization points have
          // timestamps <= max_tsc. Operations that occurred later do not need
          // to take affect yet.
          flush_logger(&way->logger_);
          cpus_.atomic_reset(cpu);
          way->obj_.store(nullptr, std::memory_order_relaxed);
          any = true;
        }
        if (!any)
          break;
        // Make sure we see concurrent updates to cpus_.
        barrier();
      }

      // Tell the logged object that it has a consistent set of
      // loggers and should do any final flushing.
      flush_finish_max_timestamp(max_tsc);

      assert(max_tsc > synced_upto_tsc);
      synced_upto_tsc = max_tsc;

      return std::move(guard);
    }

  };

  // Problems with paper API:
  // * Synchronize calls apply on each Queue object.  Where do ordered
  //   queues actually get merged?
  // * Supposedly it flushes long queues, but there's nowhere in the
  //   supposed API where that can happen.  Object::queue doesn't know
  //   the length of the queue and Queue::push can't do the right
  //   locking.
  // * Baking "Op" into the API is awkward for type-specific oplogs.
  // * Evicting a queue on hash collision is actually really
  //   complicated.  The paper says you synchronize the whole object,
  //   but the requires locking the other queues for that object,
  //   which is either racy or deadlock-prone.  For many queue types,
  //   it's perfectly reasonable to flush a single queue.  Even for
  //   queue types that require a global synchronization (e.g., to
  //   merge ordered queues), you can always flush the queue back to a
  //   per-object queue, and only apply that on sync.
  // * Queue types have no convenient way to record per-object state
  //   (e.g., evicted but unapplied operations).
  // * Type-specific Queue types don't automatically have access to
  //   the type's private fields, which is probably what they need to
  //   modify.
  // * (Not really a problem, per se) The paper frames OpLog as the
  //   TSC-ordered approach that can then be optimized for specific
  //   types.  I think this makes the API awkward, since the API is
  //   aimed at the TSC-ordered queue, rather than type-specific
  //   queues.  Another way to look at it is that OpLog handles the
  //   mechanics of per-core queues, queue caching, and
  //   synchronization and that the user can plug in any queue type by
  //   implementing a simple interface.  The TSC-ordered queue is then
  //   simply a very general queue type that the user may choose to
  //   plug in.
};
