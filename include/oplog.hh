#pragma once

#include "bitset.hh"
#include "cpu.hh"
#include "spinlock.hh"
#include "percpu.hh"
#include "cpuid.hh"

#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>
#include <algorithm>

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
    constexpr logged_object() : sync_lock_("logged_object") { }

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
    // Return a locked operation logger for this object.  In general,
    // this logger will be CPU-local, meaning that operations from
    // different cores can be performed in parallel and without
    // communication.
    locked_logger get_logger()
    {
      auto id = myid();
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
          auto sync_guard = cur_obj->sync_lock_.try_guard();
          if (!sync_guard)
            // We would deadlock with synchronize.  Back out
            goto back_out;
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
      }
      cpus_.atomic_set(id);
      return locked_logger(std::move(guard), &my_way->logger_);
    }

    // Acquire a per-object lock, apply all logged operations to this
    // object, and return the per-object lock.  The caller may keep
    // this lock live for as long as it needs to prevent modifications
    // to the object's synchronized value.
    lock_guard<spinlock> synchronize()
    {
      auto guard = sync_lock_.guard();

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

    // Per-type, per-CPU, per-object logger.  The per-CPU part of this
    // is unprotected because we lock internally.
    static percpu<cache, NO_CRITICAL> cache_;

    // Bitmask of CPUs that have logged operations for this object.
    // Bits can be set without any lock, but can only be cleared when
    // holding sync_lock_.
    bitset<NCPU> cpus_;

    // This lock serializes log flushes and protects clearing cpus_.
    spinlock sync_lock_;
  };

  template<typename Logger>
  percpu<typename logged_object<Logger>::cache, NO_CRITICAL> logged_object<Logger>::cache_; 

  // The logger class used by tsc_logged_object.
  class tsc_logger
  {
    class op
    {
    public:
      const uint64_t tsc;
      op(uint64_t tsc) : tsc(tsc) { }
      virtual void run() = 0;
      virtual void print() = 0;
    };

    template<class CB>
    class op_inst : public op
    {
      CB cb_;
    public:
      NEW_DELETE_OPS(op_inst);
      op_inst(uint64_t tsc, CB &&cb) : op(tsc), cb_(cb) { }
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
    std::vector<op*> ops_;

    static uint64_t rdtscp() 
    {
      if (cpuid::features().rdtscp)
        return rdtscp();
      return rdtsc_serialized();
    }

    void reset()
    {
      ops_.clear();
    }

    friend class tsc_logged_object;

  public:
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
      ops_.push_back(new op_inst<CB>(rdtscp(), std::forward<CB>(cb)));
    }

    // Same as push<CB>, the only difference being that the tsc value is passed
    // here instead of calling rdtscp() to get a tsc value. This is used to log
    // filesystem operations in the logical log, where the tsc is read off at
    // the linearization point of the operation (when applied on mfs).
    template<typename CB>
    void push_with_tsc(CB &&cb)
    {
      ops_.push_back(new op_inst<CB>(cb.get_tsc(), std::forward<CB>(cb)));
    }

    static bool compare_tsc(op *op1, op *op2) { return (op1->tsc < op2->tsc); }

    void sort_ops() {
      std::sort(ops_.begin(), ops_.end(), compare_tsc);
    }
    
    void print_ops() {
      for (auto it = ops_.begin(); it != ops_.end(); it++)
        (*it)->print();
    }

    size_t ops_size() { return ops_.size(); }
    op* op_at_index(int i) { return ops_.at(i); }
  };

  // A logger that applies operations in global timestamp order using
  // synchronized TSCs.
  class tsc_logged_object : public logged_object<tsc_logger>
  {
    typedef struct {
      tsc_logger::op *op;
      u64 logger_index;
    }heap_element;
    std::vector<heap_element> min_heap_; // used to heap-merge the loggers

    std::vector<tsc_logger> pending_;

    void flush_logger(tsc_logger *l) override
    {
      pending_.emplace_back(std::move(*l));
      l->reset();
    }

    void print_pending_loggers() {
      for (auto it = pending_.begin(); it != pending_.end(); it++)
        it->print_ops();
    }

    void min_heapify(int i) {
      if (min_heap_.size() <= 1)
        return;
      assert(i < min_heap_.size());
      int left = 2*i+1, right = 2*i+2, min_index = i;
      heap_element temp;
      heap_element min = min_heap_.at(i);
      if(left < min_heap_.size() && tsc_logger::compare_tsc(min_heap_.at(left).op, min.op)) {
        min = min_heap_.at(left);
        min_index = left;
      }
      if(right < min_heap_.size() && tsc_logger::compare_tsc(min_heap_.at(right).op, min.op)) {
        min = min_heap_.at(right);
        min_index = right;
      }
      if(min_index == i) // No more heap properties violated.
        return;
      // Swap with whichever child is smaller.
      temp = min_heap_.at(min_index);
      min_heap_.at(min_index) = min_heap_.at(i);
      min_heap_.at(i) = temp;
      min_heapify(min_index);
    }

    // This should heap-merge all of the loggers
    // in pending_ and apply their operations in order.
    void flush_finish() override {
      if (pending_.size() < 0)
        return;
      int size = 0, i = 0;
      std::vector<int> indices;
      std::vector<tsc_logger::op*> merged_ops;
      for(auto it = pending_.begin(); it < pending_.end(); it++) {
        it->sort_ops();  //XXX(rasha) Are the inidividual loggers already in tsc order?
        size += it->ops_size();
        indices.push_back(0);
      }

      if (size == 0)
        return;
      //Merge the operations using heaps
      i = 0;
      min_heap_.clear();
      for(auto it = pending_.begin(); it < pending_.end(); it++, i++) {
        if (it->ops_size() == 0)
          continue;
        heap_element temp;
        temp.op = it->op_at_index(0);
        temp.logger_index = i;
        min_heap_.push_back(temp);
      }
      for(i = min_heap_.size()-1; i >= 0; i--)
        min_heapify(i);
      merged_ops.push_back(min_heap_.at(0).op);

      i = 1;
      while (i < size) {
        int index = min_heap_.at(0).logger_index;
        indices.at(index)++;
        if (indices.at(index) < pending_.at(index).ops_size()) {
          heap_element temp;
          temp.op = pending_.at(index).op_at_index(indices.at(index));
          temp.logger_index = index;
          min_heap_.at(0) = temp;
          i++;
        } else {
          min_heap_.at(0) = min_heap_.back();
          min_heap_.pop_back();
        }
        if (min_heap_.size() == 0)
          break;
        min_heapify(0);
        merged_ops.push_back(min_heap_.at(0).op);
      }
 
      for(auto it = merged_ops.begin(); it < merged_ops.end(); it++) {
        ((tsc_logger::op *)(*it))->run();
      }
      for(auto it = pending_.begin(); it < pending_.end(); it++)
        it->reset();
      pending_.clear();
      min_heap_.clear();
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
