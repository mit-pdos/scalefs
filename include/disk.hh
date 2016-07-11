#pragma once

#include "spinlock.hh"
#include "condvar.hh"

struct kiovec
{
  void *iov_base;
  u64 iov_len;
};

class disk_completion : public referenced
{
public:
  disk_completion() : done_(false) {}
  NEW_DELETE_OPS(disk_completion);

  void notify() {
    scoped_acquire a(&lock_);
    done_ = true;
    cv_.wake_all();
  }

  void wait() {
    scoped_acquire a(&lock_);
    while (!done_)
      cv_.sleep(&lock_);
  }

  bool done() {
    scoped_acquire a(&lock_);
    return done_;
  }

private:
  spinlock lock_;
  condvar cv_;
  bool done_;
};

class disk
{
public:
  disk() {}
  disk(const disk &) = delete;
  disk &operator=(const disk &) = delete;

  uint64_t dk_nbytes;
  char dk_model[40];
  char dk_serial[20];
  char dk_firmware[8];
  char dk_busloc[20];

  virtual void readv(kiovec *iov, int iov_cnt, u64 off) = 0;
  virtual void writev(kiovec *iov, int iov_cnt, u64 off) = 0;
  virtual void flush() = 0;

  virtual void areadv(kiovec *iov, int iov_cnt, u64 off,
                      sref<disk_completion> dc) {
    readv(iov, iov_cnt, off);
    dc->notify();
  }
  virtual void awritev(kiovec *iov, int iov_cnt, u64 off,
                      sref<disk_completion> dc) {
    writev(iov, iov_cnt, off);
    dc->notify();
  }
  virtual void aflush(sref<disk_completion> dc) {
    flush();
    dc->notify();
  }

  void read(char* buf, u64 nbytes, u64 off) {
    kiovec iov = { (void*) buf, nbytes };
    readv(&iov, 1, off);
  }

  void aread(char* buf, u64 nbytes, u64 off,
             sref<disk_completion> dc)
  {
    kiovec iov = { (void*) buf, nbytes };
    areadv(&iov, 1, off, dc);
  }

  void write(const char* buf, u64 nbytes, u64 off) {
    kiovec iov = { (void*) buf, nbytes };
    writev(&iov, 1, off);
  }

  void awrite(const char* buf, u64 nbytes, u64 off,
              sref<disk_completion> dc)
  {
    kiovec iov = { (void*) buf, nbytes };
    awritev(&iov, 1, off, dc);
  }
};

void disk_register(disk* d);

void disk_read(u32 dev, char* data, u64 count, u64 offset,
               sref<disk_completion> dc = sref<disk_completion>());

void disk_write(u32 dev, const char* data, u64 count, u64 offset,
                sref<disk_completion> dc = sref<disk_completion>());

void disk_flush(u32 dev);
