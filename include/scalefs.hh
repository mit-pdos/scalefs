#pragma once

#include "linearhash.hh"
#include "buf.hh"
#include "cpuid.hh"
#include "oplog.hh"
#include "bitset.hh"
#include "disk.hh"
#include <vector>
#include <algorithm>

class mnode;
class transaction;
class mfs_interface;
class mfs_operation;
class mfs_operation_create;
class mfs_operation_link;
class mfs_operation_unlink;
class mfs_operation_delete;
class mfs_operation_rename_link;
class mfs_operation_rename_unlink;
class mfs_operation_rename_barrier;
class mfs_logical_log;
typedef std::vector<mfs_operation*> mfs_operation_vec;
typedef std::vector<mfs_operation*>::iterator mfs_operation_iterator;
typedef struct transaction_diskblock transaction_diskblock;

enum {
  MFS_OP_CREATE_FILE = 1,
  MFS_OP_CREATE_DIR,
  MFS_OP_LINK_FILE,
  MFS_OP_LINK_DIR,
  MFS_OP_UNLINK_FILE,
  MFS_OP_UNLINK_DIR,
  MFS_OP_RENAME_LINK_FILE,
  MFS_OP_RENAME_LINK_DIR,
  MFS_OP_RENAME_UNLINK_FILE,
  MFS_OP_RENAME_UNLINK_DIR,
  MFS_OP_RENAME_BARRIER
};

struct tx_queue_info {
  int id_;		// ID of the transaction queue
  u64 timestamp_;	// The enqueue timestamp of the transaction.

  tx_queue_info() : id_(0), timestamp_(0) {}
  tx_queue_info(int id, u64 timestamp) : id_(id), timestamp_(timestamp) {}

  tx_queue_info& operator=(const tx_queue_info&) = default;
  tx_queue_info(const tx_queue_info&) = default;

  tx_queue_info& operator=(tx_queue_info&&) = default;
  tx_queue_info(tx_queue_info&&) = default;
};

// A single disk block that was updated as the result of a transaction. All
// diskblocks that were written to during the transaction are stored as a linked
// list in the transaction object.
struct transaction_diskblock {
  u32 blocknum;           // The disk block number
  char *blockdata;        // Disk block contents
  u64 timestamp;          // Updates within a transaction should be written out
                          // in timestamp order. A single disk block might have
                          // been updated several times, but the changes not
                          // necessarily logged in timestamp order.

  sref<disk_completion> dc;

  NEW_DELETE_OPS(transaction_diskblock);

  transaction_diskblock(u32 n, char buf[BSIZE])
  {
    blockdata = (char *) kmalloc(BSIZE, "transaction_diskblock");
    blocknum = n;
    memmove(blockdata, buf, BSIZE);
    timestamp = get_tsc();
  }

  transaction_diskblock(u32 n, char buf[BSIZE], u64 blk_timestamp)
  {
    blockdata = (char *) kmalloc(BSIZE, "transaction_diskblock");
    blocknum = n;
    memmove(blockdata, buf, BSIZE);
    timestamp = blk_timestamp;
  }

  ~transaction_diskblock()
  {
    kmfree(blockdata, BSIZE);
  }

  transaction_diskblock(const transaction_diskblock&) = delete;
  transaction_diskblock& operator=(const transaction_diskblock&) = delete;

  transaction_diskblock(transaction_diskblock&& db) = default;
  transaction_diskblock& operator=(transaction_diskblock&& db) = default;

  // Write out the block contents to disk block # blocknum.
  void writeback()
  {
      disk_write(1, blockdata, BSIZE, blocknum * BSIZE);
  }

  // Write out the block contents to disk block # blocknum,
  // using asynchronous disk I/O.
  void writeback_async()
  {
      dc = make_sref<disk_completion>();
      disk_write(1, blockdata, BSIZE, blocknum * BSIZE, dc);
  }

  // Wait for the async I/O to complete.
  void async_iowait()
  {
    dc->wait();
    dc.reset();
  }

  void writeback_through_bufcache()
  {
    sref<buf> bp = buf::get(1, blocknum, true);
    {
      auto locked = bp->write();
      memmove(locked->data, blockdata, BSIZE);
    }
    // Can't use async I/O here (which uses sleep) because this is called during
    // early boot, before the process is fully setup for scheduling.
    writeback();
  }

};

// A transaction represents all related updates that take place as the result of a
// filesystem operation.
class transaction {
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(transaction);
    explicit transaction(u64 t) : timestamp_(t), htable_initialized(false),
                                  bqueue_initialized(false) {}

    transaction() : timestamp_(get_tsc()), htable_initialized(false),
                    bqueue_initialized(false) {}

    ~transaction()
    {
      if (htable_initialized)
        delete trans_blocks;

      if (bqueue_initialized)
        delete bqueue;

      for (auto &b : blocks)
        delete b;
    }

    void add_dirty_blocknum(u32 bno)
    {
      dirty_blocknums.push_back(bno);
    }

    // Add a diskblock to the transaction. These diskblocks are not necessarily
    // added in timestamp order. They should be sorted before actually flushing
    // out the changes to disk.
    void add_block(u32 bno, char buf[BSIZE])
    {
      auto b = new transaction_diskblock(bno, buf);
      add_block(std::move(b));
    }

    void add_block(transaction_diskblock *b)
    {
      blocks.push_back(std::move(b));
    }

    // Add multiple disk blocks to a transaction.
    void add_blocks(std::vector<transaction_diskblock*> bvec)
    {
      for (auto &b : bvec)
        blocks.push_back(std::move(b));
    }

    void add_free_blocks(std::vector<u32> free_list)
    {
      for (auto &f : free_list)
        free_block_list.push_back(std::move(f));
    }

    void add_free_inums(std::vector<u32> free_list)
    {
      for (auto &f : free_list)
        free_inum_list.push_back(std::move(f));
    }

    void add_allocated_block(u32 bno)
    {
      allocated_block_list.push_back(bno);
    }

    void add_free_block(u32 bno)
    {
      free_block_list.push_back(bno);
    }

    void add_free_inum(u32 inum)
    {
      free_inum_list.push_back(inum);
    }

    void add_dirty_blocks_lazy()
    {
      deduplicate_dirty_blocknums();

      for (auto &bno : dirty_blocknums) {
        sref<buf> bp = buf::get(1, bno);
        bp->add_to_transaction(this);
      }

      dirty_blocknums.clear();
    }

    void deduplicate_dirty_blocknums()
    {
      // Sort the diskblocks in increasing timestamp order.
      std::sort(dirty_blocknums.begin(), dirty_blocknums.end());

      // Make a list of the most current version of the diskblocks. For each
      // block number pick the diskblock with the highest timestamp and
      // discard the rest.

      std::vector<unsigned long> erase_indices;
      for (auto b = dirty_blocknums.begin(); b != dirty_blocknums.end(); b++) {
        if ((b+1) != dirty_blocknums.end() && (*b) == (*(b+1))) {
          erase_indices.push_back(b - dirty_blocknums.begin());
        }
      }

      std::sort(erase_indices.begin(), erase_indices.end(),
                std::greater<unsigned long>());

      for (auto &idx : erase_indices)
        dirty_blocknums.erase(dirty_blocknums.begin() + idx);
    }

    void deduplicate_freeblock_list()
    {
      std::sort(free_block_list.begin(), free_block_list.end());

      std::vector<unsigned long> erase_indices;
      for (auto b = free_block_list.begin(); b != free_block_list.end(); b++) {
        if ((b+1) != free_block_list.end() && (*b) == (*(b+1))) {
          erase_indices.push_back(b - free_block_list.begin());
        }
      }

      std::sort(erase_indices.begin(), erase_indices.end(),
                std::greater<unsigned long>());

      for (auto &idx : erase_indices)
        free_block_list.erase(free_block_list.begin() + idx);
    }

    void deduplicate_freeinum_list()
    {
      std::sort(free_inum_list.begin(), free_inum_list.end());

      std::vector<unsigned long> erase_indices;
      for (auto b = free_inum_list.begin(); b != free_inum_list.end(); b++) {
        if ((b+1) != free_inum_list.end() && (*b) == (*(b+1))) {
          erase_indices.push_back(b - free_inum_list.begin());
        }
      }

      std::sort(erase_indices.begin(), erase_indices.end(),
                std::greater<unsigned long>());

      for (auto &idx : erase_indices)
        free_inum_list.erase(free_inum_list.begin() + idx);
    }

    void deduplicate_blocks()
    {
      // Sort the diskblocks in increasing timestamp order.
      std::sort(blocks.begin(), blocks.end(), compare_transaction_db);

      // Make a list of the most current version of the diskblocks. For each
      // block number pick the diskblock with the highest timestamp and
      // discard the rest.

      std::vector<unsigned long> erase_indices;
      for (auto b = blocks.begin(); b != blocks.end(); b++) {
        if ((b+1) != blocks.end() && (*b)->blocknum == (*(b+1))->blocknum) {
          erase_indices.push_back(b - blocks.begin());
        }
      }

      std::sort(erase_indices.begin(), erase_indices.end(),
                std::greater<unsigned long>());

      for (auto &idx : erase_indices) {
        delete blocks[idx];
        blocks.erase(blocks.begin() + idx);
      }
    }

    // Comparison function to order diskblock updates. Diskblocks are ordered in
    // increasing order of (blocknum, timestamp). This ensures that all diskblocks
    // with the same blocknum are present in the list in sequence and ordered in
    // increasing order of their timestamps. So while writing out the transaction
    // to the journal, for each block number just the block with the highest
    // timestamp needs to be written to disk. Gets rid of unnecessary I/O.
    static bool compare_transaction_db(const transaction_diskblock *b1,
                                       const transaction_diskblock *b2) {
      if (b1->blocknum == b2->blocknum)
        return (b1->timestamp < b2->timestamp);
      return (b1->blocknum < b2->blocknum);
    }

    // Write a block to the disk via the transaction's block-queue.
    void write_block(u32 dev, const char *buf, u64 blocknum)
    {
      if (!bqueue_initialized) {
        bqueue = new block_queue();
        bqueue_initialized = true;
      }

      bqueue->write(dev, buf, BSIZE, blocknum * BSIZE);
      disks_written.set(blknum_to_dev(blocknum));
    }

    // Write the blocks in this transaction to disk. Used to write the journal.
    void write_to_disk()
    {
      deduplicate_blocks();

      if (!bqueue_initialized) {
        bqueue = new block_queue();
        bqueue_initialized = true;
      }

      for (auto b = blocks.begin(); b != blocks.end(); b++) {
        bqueue->write(1, (*b)->blockdata, BSIZE, (*b)->blocknum * BSIZE);
        disks_written.set(blknum_to_dev((*b)->blocknum));
      }

      // Make sure all the block-writes complete.
      bqueue->flush();
      delete bqueue;
      bqueue_initialized = false;
    }

    // Same as write_to_disk_and_flush(), except that this uses synchronous I/O,
    // and does not make the process sleep/wait (which can be troublesome at
    // early boot before the process is fully setup for scheduling).
    void write_to_disk_and_flush_raw()
    {
      write_to_disk_raw();

      for (auto d : disks_written)
        disk_flush(d);

      disks_written.reset();
    }

    void write_to_disk_and_flush()
    {
      write_to_disk();

      sref<disk_completion> dc_vec[NDISK];

      for (auto d : disks_written) {
        dc_vec[d] = make_sref<disk_completion>();
        disk_flush(d, dc_vec[d]);
      }

      for (auto d : disks_written) {
        dc_vec[d]->wait();
        dc_vec[d].reset();
      }

      disks_written.reset();
    }

    // Same as write_to_disk(), except that this uses synchronous disk I/O,
    // and does not make the process sleep/wait (which can be troublesome at
    // early boot before the process is fully setup for scheduling).
    void write_to_disk_raw()
    {
      deduplicate_blocks();

      for (auto b = blocks.begin(); b != blocks.end(); b++) {
        (*b)->writeback();
        disks_written.set(blknum_to_dev((*b)->blocknum));
      }
    }

    // Writes the blocks in the transaction to disk, and updates the
    // corresponding bufcache entries too. Used on crash recovery to avoid
    // rebooting after the changes have been applied.
    void write_to_disk_update_bufcache()
    {
      deduplicate_blocks();

      for (auto b = blocks.begin(); b != blocks.end(); b++)
        (*b)->writeback_through_bufcache();
    }

    void flush_block_queue()
    {
      if (bqueue_initialized)
        bqueue->flush();
    }

    // Transactions need to be applied in timestamp order too. They might not
    // have been logged in the journal in timestamp order.
    const u64 timestamp_;

    // Timestamp at which this transaction was enqueued to a journal's
    // transaction queue, and the queue's id.
    u64 enq_tsc; // Guaranteed to be monotonically increasing, for a given queue.
    int txq_id;

    // List of transaction queues and the transaction timestamps that this
    // transaction depends on. Applying this transaction to the disk must be
    // postponed until all these dependent transactions are applied.
    std::vector<tx_queue_info> dependent_txq;

    u64 last_group_txn_tsc;
    u64 commit_tsc;

  private:
    // List of updated diskblocks
    std::vector<transaction_diskblock*> blocks;

    std::vector<u32> dirty_blocknums;

    // Hash-table of blocks updated within the transaction. Used to ensure that
    // we don't log the same blocks repeatedly in the transaction.
    linearhash<u64, transaction_diskblock *> *trans_blocks;

    // Notes whether the trans_blocks hash table has been initialized. Used
    // to allocate memory for the hash table on-demand.
    bool htable_initialized;

    // Block numbers of newly allocated blocks within this transaction. These
    // blocks have not been marked as allocated on the disk yet.
    std::vector<u32> allocated_block_list;

    // Block numbers of blocks freed within this transaction. These blocks have
    // not been marked as free on the disk yet.
    std::vector<u32> free_block_list;

    // Inode numbers of inodes freed by this transaction. They will be made
    // available for reuse only after this transaction commits successfully.
    std::vector<u32> free_inum_list;

    // Set of inode-block and bitmap-block locks that this transaction owns.
    std::vector<u32> inodebitmap_blk_list;
    std::vector<sleeplock*> inodebitmap_locks;

    // A bitmap of disks written to by this transaction, which is used to call
    // disk_flush() on exactly those set of disks.
    bitset<NDISK> disks_written;
    block_queue *bqueue; // Access to the block layer.
    bool bqueue_initialized;
};

// The "physical" journal is made up of transactions, which in turn are made up of
// updated diskblocks.
class journal {
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(journal);
    journal() : last_applied_commit_tsc(0), current_off(0),
                committed_trans_tsc(0), applied_trans_tsc(0)
    {
      apply_dedup_trans = new transaction();
    }

    ~journal()
    {
      assert(tx_commit_queue.empty());
      assert(tx_apply_queue.empty());
    }

    // Add a new transaction to the journal's transaction commit queue.
    void enqueue_transaction(transaction *tr)
    {
      tx_commit_queue.push_back(tr);
    }

    // comparison function to order journal transactions in timestamp order
    static bool compare_txn_tsc(transaction *t1, transaction *t2)
    {
      return t1->commit_tsc < t2->commit_tsc;
    }

    u32 current_offset() {
      scoped_acquire l(&offset_lock);
      return current_off;
    }

    void update_offset(u32 new_off) {
      assert(new_off <= PHYS_JOURNAL_SIZE);
      scoped_acquire l(&offset_lock);
      current_off = new_off;
    }

    void wait_for_commit(u64 upto_enq_tsc) {
      scoped_acquire a(&commit_cv_lock_);
      while (committed_trans_tsc < upto_enq_tsc)
        commit_cv_.sleep(&commit_cv_lock_);
    }

    void wait_for_apply(u64 upto_enq_tsc) {
      scoped_acquire a(&apply_cv_lock_);
      while (applied_trans_tsc < upto_enq_tsc)
        apply_cv_.sleep(&apply_cv_lock_);
    }

    void notify_commit(u64 committed_tsc) {
      scoped_acquire a(&commit_cv_lock_);
      assert(committed_tsc > committed_trans_tsc);
      committed_trans_tsc = committed_tsc;
      commit_cv_.wake_all();
    }

    void notify_apply(u64 applied_tsc) {
      scoped_acquire a(&apply_cv_lock_);
      assert(applied_tsc > applied_trans_tsc);
      applied_trans_tsc = applied_tsc;
      apply_cv_.wake_all();
    }

    u64 get_committed_tsc() {
      scoped_acquire a(&commit_cv_lock_);
      return committed_trans_tsc;
    }

    u64 get_applied_tsc() {
      scoped_acquire a(&apply_cv_lock_);
      return applied_trans_tsc;
    }

  private:
    std::vector<transaction*> tx_commit_queue;
    std::vector<transaction*> tx_apply_queue;

    // Locks to guarantee atomic updates to the commit and apply queue
    // data-structures.
    spinlock tx_commit_queue_lock, tx_apply_queue_lock;

    // Locks to preserve high-level invariants with respect to inserting and
    // removing transactions to/from the commit and apply transaction queues
    // respectively. For example, transactions should be processed
    // (committed/applied) in the same order that they are removed from the
    // queues. So, the commitq_remove_lock (or the applyq_remove_lock) is held
    // throughout that sequence.
  public:
    sleeplock commitq_insert_lock, commitq_remove_lock;
    sleeplock applyq_insert_lock, applyq_remove_lock;

    u64 last_applied_commit_tsc;

  private:
    transaction *apply_dedup_trans;

    // Ensures that there is only one process/thread driving
    // flush_transaction_queue() on a given per-core journal at a time.
    // This is used for efficiency reasons: it avoids redundant attempts
    // at performing the same operations from multiple processes/threads,
    // thus reducing the number of sleeps/wakeups and IPIs in the commit/apply
    // path.
    sleeplock journal_lock;

    // Current size of flushed out transactions on the disk.
    u32 current_off;
    spinlock offset_lock; // Protects access to current_off.

    // The timestamp of the last transaction that was committed to the on-disk
    // filesystem via this journal.
    u64 committed_trans_tsc;
    // The timestamp of the last transaction that was applied to the on-disk
    // filesystem via this journal.
    u64 applied_trans_tsc;
    spinlock commit_cv_lock_, apply_cv_lock_;
    condvar commit_cv_, apply_cv_;
};


// This class acts as an interfacing layer between the in-memory representation
// of the filesystem and the on-disk representation. It provides functions to
// convert mnode operations to inode operations and vice versa. This also
// functions as a container for the physical and logical logs.
class mfs_interface
{
  public:

    // Headers used by the journal to indicate start and commit of transactions.
    typedef struct journal_header_block {
      journal_header_block() : timestamp(0), header_type(0) {}
      journal_header_block(u64 t, u8 bt) : timestamp(t), header_type(bt) {}

      u64 timestamp; // The transaction timestamp, serves as the transaction ID.
      u8 header_type; // The type of the journal header (start or commit)

      // The following fields are used only if this is a start block.
      u8 num_addr_blocks; // No. of address-blocks that follow the start block.
      u8 padding[2];
      u32 blocknums[1021]; // Block numbers of the data blocks in the transaction.

    } journal_header;

    static_assert(sizeof(journal_header_block) == BSIZE,
                  "Journal header size is not optimal\n");

    // Address block(s) : stores addresses (block numbers) of data blocks in the
    // transaction, which helps apply them to the on-disk filesystem after commit.
    // The first few block numbers are stored in the start header block itself.
    // Dedicated address blocks are used if the block numbers overflow from the
    // start header block.
    typedef struct journal_addr_block {
      u32 blocknums[1024];
    } journal_addr_block;

    static_assert(sizeof(journal_addr_block) == BSIZE,
                  "Journal address block size should be equal to BSIZE\n");

    static_assert((PHYS_JOURNAL_SIZE/BSIZE) <= 1021 + 1024,
                   "Add more address blocks in the transaction commit code\n");

    // Types of journal headers
    enum : u8 {
      JOURNAL_TXN_START = 1,     // Start block
      JOURNAL_TXN_COMMIT,        // Commit block
      JOURNAL_TXN_SKIP,          // Skip block
    };

    struct pending_metadata {
      u64 mnum;
      u64 max_tsc;
      int count;
    };

    struct dirunlink_metadata {
      u64 mnum;
    };

    struct rename_metadata {
      u64 src_parent_mnum;
      u64 dst_parent_mnum;
      u64 timestamp;
    };

    struct rename_barrier_metadata {
      u64 mnode_mnum;
      u64 timestamp;
    };

    // Keeps track of free and allocated blocks on the disk. Transactions that
    // modify the block free bitmap on the disk go through a list of free_bit
    // structs in memory first.
    typedef struct free_bit {
      u32 bno_;
      int cpu;
      bool is_free;
      ilink<free_bit> link;

      free_bit(u32 bno, bool f): bno_(bno), is_free(f) {}
      NEW_DELETE_OPS(free_bit);

      free_bit& operator=(const free_bit&) = delete;
      free_bit(const free_bit&) = delete;

      free_bit& operator=(free_bit&&) = default;
      free_bit(free_bit&&) = default;
    } free_bit;

    // The free block bitmap in memory. All block allocations (in transactions)
    // are performed using this in-memory data-structure. Blocks freed by a
    // transaction are freed in this bitmap *after* the transaction commits.
    // This helps us guarantee that the blocks freed by a transaction are not
    // reused until it successfully commits to disk.
    struct freeblock_bitmap {
      // We maintain the bitmap as both a vector and a linked-list so that we
      // can perform both allocations and frees in O(1) time. The bit_vector
      // contains entries for all blocks, whereas the bit_freelist contains
      // entries only for blocks that are actually free. The allocator consumes
      // items from the bit_freelist in O(1) time; the free code locates the
      // free_bit data-structure corresponding to the block being freed in O(1)
      // time using the bit_vector and inserts it into the bit_freelist (also
      // in O(1) time). Items are never removed from the bit_vector so as to
      // enable the O(1) lookups.
      std::vector<free_bit*> bit_vector;

      struct freelist {
        ilist<free_bit, &free_bit::link> bit_freelist;
        spinlock list_lock; // Guards modifications to the bit_freelist
      };

      // We maintain per-CPU freelists for scalability. The bit_vector is
      // read-only after initialization, so a single one will suffice.
      percpu<struct freelist> freelists;
      struct freelist reserve_freelist; // Global reserve pool of free blocks.
    } freeblock_bitmap;

    NEW_DELETE_OPS(mfs_interface);
    mfs_interface();

    // File functions
    u64 get_file_size(u64 mfile_mnum);
    void update_file_size(u64 mfile_mnum, u32 size, transaction *tr);
    void initialize_file(sref<mnode> m);
    int load_file_page(u64 mfile_mnum, char *p, size_t pos, size_t nbytes);
    sref<inode> prepare_sync_file_pages(u64 mfile_mnum, transaction *tr);
    int sync_file_page(sref<inode> ip, char *p, size_t pos, size_t nbytes,
                       transaction *tr);
    void finish_sync_file_pages(sref<inode> ip, transaction *tr);
    sref<inode> alloc_inode_for_mnode(u64 mnum, u8 type);
    void create_file(u64 mnum, u8 type, transaction *tr);
    void create_dir(u64 mnum, u64 parent_mnum, u8 type, transaction *tr);
    void truncate_file(u64 mfile_mnum, u32 offset, transaction *tr);

    // Directory functions
    void initialize_dir(sref<mnode> m);
    void add_dir_entry(u64 mdir_mnum, char *name, u64 dirent_mnum, u8 type,
                       transaction *tr, bool rename_link = false);
    void remove_dir_entry(u64 mdir_mnum, char* name, transaction *tr,
                          bool rename_unlink = false);
    void delete_mnum_inode_safe(u64 mnum, transaction *tr,
                                bool acquire_locks = false,
                                bool mnode_dying = false);
    void __delete_mnum_inode(u64 mnum, transaction *tr);

    bool mnum_name_insert(u64 mnum, const strbuf<DIRSIZ>& name);
    bool mnum_name_lookup(u64 mnum, strbuf<DIRSIZ> *nameptr);
    bool inum_lookup(u64 mnum, u64 *inumptr);
    sref<mnode> mnode_lookup(u64 inum, u64 *mnumptr);

    // Initializes the root directory. Called during boot.
    sref<mnode> load_root();

    // Journal functions
    void add_transaction_to_queue(transaction *tr, int cpu);
    void pre_process_transaction(transaction *tr);
    void post_process_transaction(transaction *tr);
    void apply_trans_on_disk(transaction *tr);
    void commit_transaction_to_disk(int cpu, transaction *trans);
    void apply_transaction_to_disk(int cpu, transaction *trans);
    void commit_all_transactions(int cpu);
    void apply_all_transactions(int cpu);
    void flush_transaction_queue(int cpu, bool apply_transactions = false);
    void print_txq_stats();
    bool fits_in_journal(size_t num_trans_blocks, int cpu);
    void write_journal(char *buf, size_t size, transaction *tr, int cpu);
    void write_journal_transaction_blocks(const
           std::vector<transaction_diskblock*> &vec, const u64 timestamp,
           bitset<NDISK> &disks_written, int cpu);
    void write_journal_commit_block(u64 timestamp, int cpu);
    void write_journal_skip_block(u64 timestamp, int cpu,
                                  bool use_async_io = true);

    bool get_txn_skip_block(int cpu, u64 *skip_upto_tsc);
    journal_header *get_txn_start_block(int cpu);
    bool get_txn_data_blocks(int cpu, journal_header *hdstartptr,
                             transaction *trans);
    bool get_txn_commit_block(int cpu, transaction *trans);
    void recover_journal(int cpu, std::vector<transaction*> &trans_vec);
    void reset_journal(int cpu);
    void init_journal(int cpu);

    // Metadata functions
    void alloc_mnode_lock(u64 mnum);
    void free_mnode_lock(u64 mnum);
    void alloc_metadata_log(u64 mnum);
    void free_metadata_log(u64 mnum);
    lock_guard<sleeplock> metadata_op_lockguard(u64 mnum, int cpu);
    void metadata_op_start(u64 mnum, int cpu, u64 tsc_val);
    void metadata_op_end(u64 mnum, int cpu, u64 tsc_val);
    void add_to_metadata_log(u64 mnum, int cpu, mfs_operation *op);
    void inc_mfslog_linkcount(u64 mnum);
    void dec_mfslog_linkcount(u64 mnum);
    u64  get_mfslog_linkcount(u64 mnum);
    void sync_dirty_files_and_dirs(int cpu, std::vector<u64> &mnum_list);
    void evict_bufcache();
    void evict_pagecache();
    void process_metadata_log_and_flush(int cpu);
    void process_metadata_log(u64 max_tsc, u64 mnode_mnum, int cpu);
    void add_op_to_transaction_queue(mfs_operation *op, int cpu,
                                     transaction *tr = nullptr,
                                     bool skip_add = false);
    void absorb_file_link_unlink(mfs_logical_log *mfs_log,
                                 std::vector<u64> &absorb_mnum_list);
    void absorb_delete_inode(mfs_logical_log *mfs_log, u64 mnum, int cpu,
                             std::vector<u64> &unlink_mnum_list);
    int  process_ops_from_oplog(mfs_logical_log *mfs_log, u64 max_tsc, int count,
                  int cpu,
                  std::vector<pending_metadata> &pending_stack,
                  std::vector<u64> &unlink_mnum_list,
                  std::vector<dirunlink_metadata> &dirunlink_stack,
                  std::vector<rename_metadata> &rename_stack,
                  std::vector<rename_barrier_metadata> &rename_barrier_stack,
                  std::vector<u64> &absorb_mnum_list);
    void apply_rename_pair(std::vector<rename_metadata> &rename_stack, int cpu);
    void mfs_create(mfs_operation_create *op, transaction *tr);
    void mfs_link(mfs_operation_link *op, transaction *tr);
    void mfs_unlink(mfs_operation_unlink *op, transaction *tr);
    void mfs_rename_link(mfs_operation_rename_link *op, transaction *tr);
    void mfs_rename_unlink(mfs_operation_rename_unlink *op, transaction *tr);
    void reclaim_unreachable_inodes();

    // Block allocator functionality
    void initialize_freeblock_bitmap();
    u32  alloc_block();
    void free_block(u32 bno);
    void print_free_blocks(print_stream *s);

    enum {
      INODE_BLOCK = 1,
      BITMAP_BLOCK,
    };

    void alloc_inodebitmap_locks();
    void acquire_inodebitmap_locks(std::vector<u64> &num_list, int type,
                                   transaction *tr);
    void release_inodebitmap_locks(transaction *tr);

    void preload_oplog();

  private:
    void load_dir(sref<inode> i, sref<mnode> m);
    sref<mnode> load_dir_entry(u64 inum, sref<mnode> parent);
    sref<mnode> mnode_alloc(u64 inum, u8 mtype);
    sref<inode> get_inode(u64 mnum, const char *str);
    // Mapping from disk inode numbers to the corresponding mnode numbers
    chainhash<u64, u64> *inum_to_mnum;
    // Mapping from in-memory mnode numbers to disk inode numbers
    chainhash<u64, u64> *mnum_to_inum;
    chainhash<u64, sleeplock*> *mnum_to_lock;
    chainhash<u64, strbuf<DIRSIZ>> *mnum_to_name;

    typedef struct mfs_op_idx {
      int create_index;
      int link_index;
      int unlink_index;
      int rename_index;
      int delete_index;

      mfs_op_idx() : create_index(-1), link_index(-1), unlink_index(-1),
                     rename_index(-1), delete_index(-1) {}
    } mfs_op_idx;

    // Hash-table to absorb deletes with create/link/unlink/rename
    // of the same mnode in the transaction log.
    // Mapping from mnode number to indices of the transactions corresponding
    // to that mnode in the transaction log.
    linearhash<u64, mfs_op_idx> *prune_trans_log;

  public:
    percpu<journal*> fs_journal;
    percpu<sref<inode> > sv6_journal;

    // A hash-table to track the last transaction(*) that modified a given
    // inode-block or bitmap-block. (* = specifically, which journal's
    // transaction-queue that transaction went into and at what timestamp).
    chainhash<u32, tx_queue_info> *blocknum_to_queue;

    // List of mnums whose mnodes have hit mnode::onzero() and hence their
    // corresponding on-disk inodes can be deleted.
    struct delete_inums {
      std::vector<u64> mnum_list;
      spinlock lock;
    };
    percpu<delete_inums> delete_inums;


  private:
    chainhash<u64, mfs_logical_log*> *metadata_log_htab; // The logical log

    // Set of locks, one per inode-block and one per bitmap-block.
    std::vector<sleeplock*> inodebitmap_locks;
};

class mfs_operation
{
  public:
    NEW_DELETE_OPS(mfs_operation);

    mfs_operation(mfs_interface *p, u64 t, int op_type)
      : parent_mfs(p), timestamp(t), operation_type(op_type)
    {
      assert(parent_mfs);
    }

    virtual ~mfs_operation() {}
    virtual void apply(transaction *tr) = 0;
    virtual void print() = 0;

  protected:
    mfs_interface *parent_mfs;
  public:
    const u64 timestamp;
    int operation_type; // MFS_OP_CREATE_FILE, MFS_OP_LINK_DIR etc.
};

class mfs_operation_create: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_create);

    mfs_operation_create(mfs_interface *p, u64 t, u64 mnum, u64 pt, char nm[],
                         short m_type)
      : mfs_operation(p, t, (m_type == T_DIR) ?
                            MFS_OP_CREATE_DIR : MFS_OP_CREATE_FILE),
        mnode_mnum(mnum), parent_mnum(pt), mnode_type(m_type)
    {
      name = new char[DIRSIZ];
      strncpy(name, nm, DIRSIZ);
    }

    ~mfs_operation_create()
    {
      delete[] name;
    }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_create(this, tr);
    }

    void print()
    {
      cprintf("CREATE\n");
      cprintf("Op Type : Create\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Parent Mnode Num: %ld\n", parent_mnum);
      cprintf("Name: %s\n", name);
      cprintf("Mnode type: %d\n", mnode_type);
    }

  private:
    u64 mnode_mnum;   // mnode number of the new file/directory
    u64 parent_mnum;  // mnode number of the parent directory
    char *name;       // name of the new file/directory
    short mnode_type; // type for the new mnode
};

class mfs_operation_link: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_link);

    mfs_operation_link(mfs_interface *p, u64 t, u64 mnum, u64 pt, char nm[],
                       short m_type)
      : mfs_operation(p, t, (m_type == T_DIR) ?
                            MFS_OP_LINK_DIR : MFS_OP_LINK_FILE),
        mnode_mnum(mnum), parent_mnum(pt), mnode_type(m_type)
    {
      name = new char[DIRSIZ];
      strncpy(name, nm, DIRSIZ);
    }

    ~mfs_operation_link()
    {
      delete[] name;
    }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_link(this, tr);
    }

    void print()
    {
      cprintf("LINK\n");
      cprintf("Op Type : Link\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Parent Mnode Num: %ld\n", parent_mnum);
      cprintf("Name: %s\n", name);
      cprintf("Mnode type: %d\n", mnode_type);
    }

  private:
    u64 mnode_mnum;   // mnode number of the file/directory to be linked
    u64 parent_mnum;  // mnode number of the parent directory
    char *name;       // name of the file/directory
    short mnode_type; // type of the mnode (file/dir)
};

class mfs_operation_unlink: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_unlink);

    mfs_operation_unlink(mfs_interface *p, u64 t, u64 mnum, u64 pt, char nm[],
                         short m_type)
      : mfs_operation(p, t, (m_type == T_DIR) ?
                            MFS_OP_UNLINK_DIR : MFS_OP_UNLINK_FILE),
        mnode_mnum(mnum), parent_mnum(pt), mnode_type(m_type)
    {
      name = new char[DIRSIZ];
      strncpy(name, nm, DIRSIZ);
    }

    ~mfs_operation_unlink()
    {
      delete[] name;
    }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_unlink(this, tr);
    }

    void print()
    {
      cprintf("UNLINK\n");
      cprintf("Op Type : Unlink\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Parent Mnode Num: %ld\n", parent_mnum);
      cprintf("Name: %s\n", name);
      cprintf("Mnode type: %d\n", mnode_type);
    }

  private:
    u64 mnode_mnum;   // mnode number of the file/directory to be unlinked
    u64 parent_mnum;  // mnode number of the parent directory
    char *name;       // name of the file/directory
    short mnode_type; // type of the mnode (file/dir)
};

class mfs_operation_rename_link: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_rename_link);

    mfs_operation_rename_link(mfs_interface *p, u64 t, char oldnm[], u64 mnum,
                              u64 src_pt, char newnm[], u64 dst_pt, u8 m_type)
      : mfs_operation(p, t, (m_type == T_DIR) ?
                            MFS_OP_RENAME_LINK_DIR : MFS_OP_RENAME_LINK_FILE),
        mnode_mnum(mnum), src_parent_mnum(src_pt), dst_parent_mnum(dst_pt),
        mnode_type(m_type)
    {
      name = new char[DIRSIZ];
      newname = new char[DIRSIZ];
      strncpy(name, oldnm, DIRSIZ);
      strncpy(newname, newnm, DIRSIZ);
    }

    ~mfs_operation_rename_link()
    {
      delete[] name;
      delete[] newname;
    }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_rename_link(this, tr);
    }

    void print()
    {
      cprintf("RENAME LINK\n");
      cprintf("Op Type : Rename Link\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Name: %s\n", name);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Src Parent Mnode Num: %ld\n", src_parent_mnum);
      cprintf("New Name: %s\n", newname);
      cprintf("Dst Parent Mnode Num: %ld\n", dst_parent_mnum);
      cprintf("Mnode type: %d\n", mnode_type);
    }

  private:
    u64 mnode_mnum;        // mnode number of the file/directory to be moved
    u64 src_parent_mnum;   // mnode number of the source directory
    u64 dst_parent_mnum;   // mnode number of the destination directory
    short mnode_type;      // type of the mnode
    char *name;            // source name
    char *newname;         // destination name
};

class mfs_operation_rename_unlink: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_rename_unlink);

    mfs_operation_rename_unlink(mfs_interface *p, u64 t, char oldnm[], u64 mnum,
                                u64 src_pt, char newnm[], u64 dst_pt, u8 m_type)
      : mfs_operation(p, t, (m_type == T_DIR) ?
                            MFS_OP_RENAME_UNLINK_DIR : MFS_OP_RENAME_UNLINK_FILE),
        mnode_mnum(mnum), src_parent_mnum(src_pt), dst_parent_mnum(dst_pt),
        mnode_type(m_type)
    {
      name = new char[DIRSIZ];
      newname = new char[DIRSIZ];
      strncpy(name, oldnm, DIRSIZ);
      strncpy(newname, newnm, DIRSIZ);
    }

    ~mfs_operation_rename_unlink()
    {
      delete[] name;
      delete[] newname;
    }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_rename_unlink(this, tr);
    }

    void print()
    {
      cprintf("RENAME UNLINK\n");
      cprintf("Op Type : Rename Unlink\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Name: %s\n", name);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Src Parent Mnode Num: %ld\n", src_parent_mnum);
      cprintf("New Name: %s\n", newname);
      cprintf("Dst Parent Mnode Num: %ld\n", dst_parent_mnum);
      cprintf("Mnode type: %d\n", mnode_type);
    }

  private:
    u64 mnode_mnum;        // mnode number of the file/directory to be moved
    u64 src_parent_mnum;   // mnode number of the source directory
    u64 dst_parent_mnum;   // mnode number of the destination directory
    short mnode_type;      // type of the mnode
    char *name;            // source name
    char *newname;         // destination name
};

// Rename barriers are used only for directory renames, in order to avoid
// forming orphaned loops on the disk due to rename and fsync.
class mfs_operation_rename_barrier: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_rename_barrier);

    mfs_operation_rename_barrier(mfs_interface *p, u64 t, u64 mnum,
                                u64 parent, u8 m_type)
      : mfs_operation(p, t, MFS_OP_RENAME_BARRIER), mnode_mnum(mnum),
        parent_mnum(parent), mnode_type(m_type)
    {}

    void apply(transaction *tr) override {}

    void print()
    {
      cprintf("RENAME BARRIER\n");
      cprintf("Op Type : Rename Barrier\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Parent Mnode Num: %ld\n", parent_mnum);
      cprintf("Mnode type: %d\n", mnode_type);
    }

  private:
    u64 mnode_mnum;        // mnode number of this directory
    u64 parent_mnum;       // mnode number of the parent of this directory at
                           // the time of performing the directory rename
    short mnode_type;      // type of the mnode
};

// The "logical" log of metadata operations. These operations are applied on an fsync
// call so that any previous dependencies can be resolved before the mnode is fsynced.
// The list of operations is oplog-maintained.
class mfs_logical_log: public mfs_logged_object
{
  friend mfs_interface;

  public:
    NEW_DELETE_OPS(mfs_logical_log);
    // Set 'use_sleeplock' to false in mfs_logged_object.
    mfs_logical_log(u64 mnum) : mfs_logged_object(false), mnode_mnum(mnum),
                                last_synced_tsc(0) {}
    ~mfs_logical_log()
    {
      for (auto it = operation_vec.begin(); it != operation_vec.end(); it++)
        delete (*it);
    }

    // Oplog operation that implements adding a metadata operation to the log
    struct add_op {
      add_op(mfs_logical_log *l, mfs_operation *op): parent(l), operation(op)
      {
        assert(l);
      }

      void operator()()
      {
        assert(operation->timestamp >= parent->last_synced_tsc);
        parent->last_synced_tsc = operation->timestamp;
        parent->operation_vec.push_back(std::move(operation));
      }

      void print()
      {
        operation->print();
      }

      u64 get_timestamp()
      {
        return operation->timestamp;
      }

      private:
      mfs_logical_log *parent;
      mfs_operation *operation;
    };

    void add_operation(mfs_operation *op, int cpu)
    {
      get_logger(cpu)->push_with_tsc<add_op>(add_op(this, op));
    }

    // This function pre-allocates a per-core logger for this core,
    // and sets the bit corresponding to this CPU in oplog's bitmap.
    // This helps avoid extraneous sharing reports when using commuter.
    void preload_oplog()
    {
      get_logger(myid());
    }

  protected:
    mfs_operation_vec operation_vec;
    sleeplock lock;
    u64 mnode_mnum;
    u64 last_synced_tsc;

    // This link-count is used to perform a hand-shake between MemFS and
    // fsync/sync for accurate zero-detection of file link-counts, which helps
    // determine when it is safe to delete the inode on the disk.
    // This counter is incremented every time this mnode is linked in MemFS,
    // and decremented in the sync/fsync path when an unlink is processed (or
    // absorbed) corresponding to this mnode(inode).
    percpu<u64> link_count;
    percpu<spinlock> link_lock; // Protects the link_count
};
