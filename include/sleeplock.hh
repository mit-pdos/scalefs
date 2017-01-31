#pragma once

#include "spinlock.hh"
#include "condvar.hh"

class sleeplock {
 public:
  NEW_DELETE_OPS(sleeplock);
  sleeplock() : held_(false) {}

  void check_locking_context_is_safe() {
    if (mycpu()->ncli != 0)
      panic("Possible nesting of sleeplock inside a spinlock!\n");
  }

  void acquire() {
    check_locking_context_is_safe();
    scoped_acquire x(&spinlock_);
    while (held_)
      cv_.sleep(&spinlock_);
    held_ = true;
  }

  bool try_acquire() {
    // We don't call check_locking_context_is_safe() here to avoid
    // false-positives: it _is_ safe to try-acquire a sleeplock while
    // holding a spinlock.
    scoped_acquire x(&spinlock_);
    if (held_)
      return false;
    held_ = true;
    return true;
  }

  void release() {
    scoped_acquire x(&spinlock_);
    assert(held_);
    held_ = false;
    cv_.wake_all();
  }

  lock_guard<sleeplock> guard() {
    return lock_guard<sleeplock>(this);
  }

  lock_guard<sleeplock> try_guard() {
    return lock_guard<sleeplock>(this, lock_guard<sleeplock>::try_guard_tag);
  }

  // Sleeplocks cannot be copied.
  sleeplock(const sleeplock &o) = delete;
  sleeplock &operator=(const sleeplock &o) = delete;

  // Sleeplocks can be moved.
  sleeplock(sleeplock &&o) = default;
  sleeplock &operator=(sleeplock &&o) = default;

 private:
  spinlock spinlock_;
  condvar cv_;
  bool held_;
};
