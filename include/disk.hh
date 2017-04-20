#pragma once

#include "spinlock.hh"
#include "condvar.hh"

#define IOV_MAX     65535    // Limited by MAX_PRD_ENTRIES
#define SG_IO_SIZE  64*1024  // Size used for scatter-gather I/O

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


u32 blknum_to_dev(u32 blknum);
u32 remap_blknum(u32 blknum);
u32 num_disks();

void disk_register(disk* d);

void disk_read(u32 dev, char* buf, u64 nbytes, u64 offset,
               sref<disk_completion> dc = sref<disk_completion>());

void disk_readv(u32 dev, kiovec *iov, int iov_cnt, u64 offset,
                sref<disk_completion> dc = sref<disk_completion>());

void disk_write(u32 dev, const char* buf, u64 nbytes, u64 offset,
                sref<disk_completion> dc = sref<disk_completion>());

void disk_writev(u32 dev, kiovec *iov, int iov_cnt, u64 offset,
                 sref<disk_completion> dc = sref<disk_completion>());

void disk_flush(u32 dev, sref<disk_completion> dc = sref<disk_completion>());


// A simple block layer for ScaleFS/sv6, that helps accumulate I/O to contiguous
// blocks, and issues them to the disk driver in large chunks so as to achieve
// high throughput disk I/O.  Contiguous writes (i.e., writes to adjacent disk
// blocks) from the filesystem are buffered upto a specified I/O size
// (SG_IO_SIZE, which is currently 64KB), and written out to disk using
// scatter-gather I/O APIs exposed by the disk driver. For simplicity, the block
// queue implementation assumes that the filesystem issues writes in increasing
// order of disk block numbers; this allows the block queue to simply accumulate
// writes as long as they are contiguous, and flush them out as soon as a write
// operation is encountered that is not contiguous with the previous write, or
// if the write-buffer fills up to SG_IO_SIZE. This is particularly effective
// for ScaleFS, as transactions already sort their disk blocks as part of their
// deduplication procedure, before writing them out. The block layer also
// transparently handles I/O to multiple disks using the preconfigured striping
// parameters, thus completely shielding the details of I/O to multiple disks
// from the higher layers (i.e., the filesystem).
//
// The block-queue class can be instantiated afresh for any "write-context",
// i.e., any code that wants to perform a set of writes to the disk. This design
// provides enormous flexibility in how it can be used - for example, every
// transaction that wants to write out its disk blocks can create its own
// private instance of the block layer to do contiguous I/O; on the other hand,
// we can also have a global, system-wide instance that accumulates writes from
// all processes/threads so as to exploit opportunities for contiguous disk I/O
// across process boundaries. And of course, any combination of these techniques
// can be used as well.
class block_queue {

public:
  NEW_DELETE_OPS(block_queue);

  block_queue()
  {
    for (int i = 0; i < num_disks(); i++)
      dqueue[i] = new disk_queue(i);
  }

  ~block_queue()
  {
    for (int i = 0; i < num_disks(); i++)
      delete dqueue[i];
  }

  void write(u32 dev, const char *buf, u64 nbytes, u64 offset)
  {
    assert(nbytes == BSIZE && offset % BSIZE == 0);

    // Remap offset to the correct disknum and offset when using multiple disks.
    dev = blknum_to_dev(offset/BSIZE);
    offset = (u64) remap_blknum(offset/BSIZE) * BSIZE;

    dqueue[dev]->add_to_queue(buf, nbytes, offset);
  }

  void flush()
  {
    for (int i = 0; i < num_disks(); i++)
      dqueue[i]->flush();
  }

private:

  class disk_queue {

  public:
    NEW_DELETE_OPS(disk_queue);

    explicit disk_queue(u32 dev) : iovec_idx(0), dev_(dev)
    {
      for (int i = 0; i < AHCI_QUEUE_DEPTH; i++) {
        start_offset[i] = 0;
        iovec[i].reserve(SG_IO_SIZE/BSIZE);
      }
    }

    ~disk_queue()
    {
      for (int i = 0; i < AHCI_QUEUE_DEPTH; i++)
        assert(iovec[i].empty());
    }

    void add_to_queue(const char *buf, u64 nbytes, u64 offset)
    {
      // Flush the existing buffer if this write is going to make it
      // discontiguous.
      if (!iovec[iovec_idx].empty() &&
          offset != start_offset[iovec_idx] + iovec[iovec_idx].size() * BSIZE) {
        flush_queue(iovec_idx);
	iovec_idx = (iovec_idx + 1) % AHCI_QUEUE_DEPTH;
      }

      kiovec kiov = { (void *)buf, nbytes };
      iovec[iovec_idx].push_back(kiov);

      // Note down the original offset if this was the first entry in the queue.
      if (iovec[iovec_idx].size() == 1)
        start_offset[iovec_idx] = offset;

      // If we accumulated SG_IO_SIZE worth of data, then write it to the disk.
      if (iovec[iovec_idx].size() == SG_IO_SIZE/BSIZE) {
        flush_queue(iovec_idx);
        iovec_idx = (iovec_idx + 1) % AHCI_QUEUE_DEPTH;
      }
    }

    void flush_queue(int iovec_idx, bool sync = false)
    {
      if (!iovec[iovec_idx].empty()) {

	// Wait for any previous write issued via this command slot to complete,
	// before issuing a new write via the same slot. (Note: This is an
	// approximation; it may or may not map exactly to the same command
	// slots in the low-level AHCI driver.)
        if (dc[iovec_idx].get()) {
          dc[iovec_idx]->wait();
          dc[iovec_idx].reset();
        }

        dc[iovec_idx] = make_sref<disk_completion>();
        disk_writev(dev_, &iovec[iovec_idx][0], iovec[iovec_idx].size(),
                    start_offset[iovec_idx], dc[iovec_idx]);
        iovec[iovec_idx].clear();
        iovec[iovec_idx].reserve(SG_IO_SIZE/BSIZE);
      }

      // For synchronous writes, wait for the completion of the write that we
      // just issued above.
      if (sync) {
        if (dc[iovec_idx].get()) {
          dc[iovec_idx]->wait();
          dc[iovec_idx].reset();
        }
      }
    }

    void flush()
    {
      for (int i = 0; i < AHCI_QUEUE_DEPTH; i++)
        flush_queue(i, true);
    }

  private:
    // Maximum number of command-slots or the NCQ queue depth, as defined in
    // the AHCI specification.
    enum { AHCI_QUEUE_DEPTH = 32 };

    std::vector<kiovec> iovec[AHCI_QUEUE_DEPTH];
    sref<disk_completion> dc[AHCI_QUEUE_DEPTH];
    u64 start_offset[AHCI_QUEUE_DEPTH];
    int iovec_idx; // Indicates which iovec to add items to next.
    u32 dev_;
  };

private:
  disk_queue* dqueue[NDISK];
};
