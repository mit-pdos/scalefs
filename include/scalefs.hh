#pragma once

#include "linearhash.hh"
#include "buf.hh"
#include "cpuid.hh"
#include "oplog.hh"
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
class mfs_operation_rename;
class mfs_logical_log;
typedef std::vector<mfs_operation*> mfs_operation_vec;
typedef std::vector<mfs_operation*>::iterator mfs_operation_iterator;
typedef struct transaction_diskblock transaction_diskblock;

static u64
get_timestamp()
{
  if (cpuid::features().rdtscp)
    return rdtscp();
  // cpuid + rdtsc combination (more expensive) is used as an alternative to
  // rdtscp where it's not supported.
  return rdtsc_serialized();
}

// A single disk block that was updated as the result of a transaction. All
// diskblocks that were written to during the transaction are stored as a linked
// list in the transaction object.
struct transaction_diskblock
{
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
    timestamp = get_timestamp();
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
      idewrite(1, blockdata, BSIZE, blocknum*BSIZE);
  }

  // Write out the block contents to disk block # blocknum,
  // using asynchronous disk I/O.
  void writeback_async()
  {
      dc = make_sref<disk_completion>();
      idewrite_async(1, blockdata, BSIZE, blocknum*BSIZE, dc);
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
    bp->writeback_async();
  }

};

// A transaction represents all related updates that take place as the result of a
// filesystem operation.
class transaction
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(transaction);
    explicit transaction(u64 t) : timestamp_(t), htable_initialized(false) {}

    transaction() : timestamp_(get_timestamp()), htable_initialized(false) {}

    ~transaction()
    {
      if (htable_initialized)
        delete trans_blocks;
    }

    // Add a diskblock to the transaction. These diskblocks are not necessarily
    // added in timestamp order. They should be sorted before actually flushing
    // out the changes to disk.
    void add_block(u32 bno, char buf[BSIZE])
    {
      auto b = std::make_unique<transaction_diskblock>(bno, buf);
      add_block(std::move(b));
    }

    void add_block(std::unique_ptr<transaction_diskblock> b)
    {
      auto l = write_lock.guard();
      blocks.push_back(std::move(b));
    }

    // Add a unique diskblock to the transaction. Never add the same diskblock
    // (i.e, the same block-number) twice, even if it has different contents.
    // Instead, update that existing diskblock with the new contents.
    void add_unique_block(u32 bno, char buf[BSIZE])
    {
      transaction_diskblock *tdp;
      u64 new_timestamp = get_timestamp();
      u64 blocknum = bno;

      if (!htable_initialized) {
        assert(blocks.size() == 0);
        // XXX: What would be the ideal size for this hash table?
        trans_blocks = new linearhash<u64, transaction_diskblock *>(4099);
        htable_initialized = true;
      }

      // Lookup the hash-table to see if we have already logged that block in
      // this transaction.
      if (trans_blocks->lookup(blocknum, &tdp)) {

        // Nothing to do if that logged block already has the latest contents.
        if (tdp->timestamp > new_timestamp)
          return;

        // Update the transaction-diskblock with new contents and re-insert it
        // into the hash table.
        trans_blocks->remove(blocknum);
        {
          auto l = write_lock.guard();
          memmove(tdp->blockdata, buf, BSIZE);
          tdp->timestamp = new_timestamp;
        }
        trans_blocks->insert(blocknum, tdp);

      } else {
        // This is the first time this diskblock is being logged in the
        // transaction. So create a new one.
        auto b = std::make_unique<transaction_diskblock>(bno, buf);

        // Get a raw pointer to that block (don't grab its ownership).
        tdp = b.get();

        add_block(std::move(b));

        // Now insert a (non-owning) pointer to that block in the hash-table.
        trans_blocks->insert(blocknum, tdp);
      }
    }

    void add_unique_block(transaction_diskblock *b)
    {
      transaction_diskblock *tdp;
      u64 blocknum = b->blocknum;
      u64 new_timestamp = b->timestamp;

      if (!htable_initialized) {
        assert(blocks.size() == 0);
        // XXX: What would be the ideal size for this hash table?
        trans_blocks = new linearhash<u64, transaction_diskblock *>(4099);
        htable_initialized = true;
      }

      // Lookup the hash-table to see if we have already logged that block in
      // this transaction.
      if (trans_blocks->lookup(blocknum, &tdp)) {

        // Nothing to do if that logged block already has the latest contents.
        if (tdp->timestamp > new_timestamp)
          return;

        // Update the transaction-diskblock with new contents and re-insert it
        // into the hash table.
        trans_blocks->remove(blocknum);
        {
          auto l = write_lock.guard();
          memmove(tdp->blockdata, b->blockdata, BSIZE);
          tdp->timestamp = new_timestamp;
        }
        trans_blocks->insert(blocknum, tdp);

      } else {
        // This is the first time this diskblock is being logged in the
        // transaction. So create a new one.
        auto blk = std::make_unique<transaction_diskblock>(blocknum,
                                           b->blockdata, new_timestamp);

        // Get a raw pointer to that block (don't grab its ownership).
        tdp = blk.get();

        add_block(std::move(blk));

        // Now insert a (non-owning) pointer to that block in the hash-table.
        trans_blocks->insert(blocknum, tdp);
      }
    }

    // Add multiple disk blocks to a transaction.
    void add_blocks(std::vector<std::unique_ptr<transaction_diskblock> > bvec)
    {
      auto l = write_lock.guard();
      for (auto &b : bvec)
        blocks.push_back(std::move(b));
    }

    void add_allocated_block(u32 bno)
    {
      allocated_block_list.push_back(bno);
    }

    void add_free_block(u32 bno)
    {
      free_block_list.push_back(bno);
    }

    // Prepare the transaction for two phase commit. The transaction diskblocks
    // are ordered by timestamp. (This is needed to ensure that multiple changes
    // to the same block are written to disk in the correct order).
    void prepare_for_commit()
    {
      // All relevant blocks must have been added to the transaction at
      // this point. A try acquire must succeed.
      auto l = write_lock.try_guard();
      assert(static_cast<bool>(l));
    }

    void finish_after_commit()
    {
      write_lock.release();
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

      for (auto &idx : erase_indices)
        blocks.erase(blocks.begin() + idx);
    }

    // Comparison function to order diskblock updates. Diskblocks are ordered in
    // increasing order of (blocknum, timestamp). This ensures that all diskblocks
    // with the same blocknum are present in the list in sequence and ordered in
    // increasing order of their timestamps. So while writing out the transaction
    // to the journal, for each block number just the block with the highest
    // timestamp needs to be written to disk. Gets rid of unnecessary I/O.
    static bool compare_transaction_db(const
    std::unique_ptr<transaction_diskblock>& b1, const
    std::unique_ptr<transaction_diskblock>& b2) {
      if (b1->blocknum == b2->blocknum)
        return (b1->timestamp < b2->timestamp);
      return (b1->blocknum < b2->blocknum);
    }

    // Write the blocks in this transaction to disk. Used to write the journal.
    void write_to_disk()
    {
      for (auto b = blocks.begin(); b != blocks.end(); b++)
        (*b)->writeback_async();

      for (auto b = blocks.begin(); b != blocks.end(); b++)
        (*b)->async_iowait();

      ideflush();
    }

    // Writes the blocks in the transaction to disk, and updates the
    // corresponding bufcache entries too. Used on crash recovery to avoid
    // rebooting after the changes have been applied.
    void write_to_disk_update_bufcache()
    {
      for (auto b = blocks.begin(); b != blocks.end(); b++)
        (*b)->writeback_through_bufcache();
    }

    // Transactions need to be applied in timestamp order too. They might not
    // have been logged in the journal in timestamp order.
    const u64 timestamp_;

  private:
    // List of updated diskblocks
    std::vector<std::unique_ptr<transaction_diskblock> > blocks;

    // Hash-table of blocks updated within the transaction. Used to ensure that
    // we don't log the same blocks repeatedly in the transaction.
    linearhash<u64, transaction_diskblock *> *trans_blocks;

    // Notes whether the trans_blocks hash table has been initialized. Used
    // to allocate memory for the hash table on-demand.
    bool htable_initialized;

    // Guards updates to the transaction_diskblock vector.
    sleeplock write_lock;

    // Block numbers of newly allocated blocks within this transaction. These
    // blocks have not been marked as allocated on the disk yet.
    std::vector<u32> allocated_block_list;

    // Block numbers of blocks freed within this transaction. These blocks have
    // not been marked as free on the disk yet.
    std::vector<u32> free_block_list;
};

// The "physical" journal is made up of transactions, which in turn are made up of
// updated diskblocks.
class journal
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(journal);
    journal()
    {
      current_off = 0;
      transaction_log = std::vector<transaction*>();
    }

    ~journal()
    {
      for (auto it = transaction_log.begin(); it != transaction_log.end(); it++)
        delete (*it);
    }

    // Add a new transaction to the journal.
    void add_transaction_locked(transaction *tr)
    {
      transaction_log.push_back(tr);
    }

    lock_guard<sleeplock> prepare_for_commit()
    {
      auto l = write_lock.guard();

      // The transactions are present in the transaction log in the order in
      // which the metadata operations were applied. This corresponds to the
      // correct linearization of the memfs operations. So the transactions are
      // logged in the correct order.

      return l;
    }

    // comparison function to order journal transactions in timestamp order
    static bool compare_timestamp_tr(transaction *t1, transaction *t2)
    {
      return (t1->timestamp_ < t2->timestamp_);
    }

    u32 current_offset() { return current_off; }
    void update_offset(u32 new_off) { current_off = new_off; }

  private:
    // List of transactions (Unordered).
    std::vector<transaction*> transaction_log;
    // Guards updates to the transaction_log
    sleeplock write_lock;
    // Current size of flushed out transactions on the disk
    u32 current_off;
};

// This class acts as an interfacing layer between the in-memory representation
// of the filesystem and the on-disk representation. It provides functions to
// convert mnode operations to inode operations and vice versa. This also
// functions as a container for the physical and logical logs.
class mfs_interface
{
  public:

    // Header used by each journal block
    typedef struct journal_block_header {
      u64 timestamp;          // The transaction timestamp, serves as the
                              // transaction ID
      u32 blocknum;           // The disk block number if this is a data block
      u8 block_type;          // The type of the journal block

      journal_block_header(u64 t, u32 n, u8 bt) {
        timestamp = t;
        blocknum = n;
        block_type = bt;
      }

      journal_block_header(): timestamp(0), blocknum(0), block_type(0) {}

    } journal_block_header;

    // Types of journal blocks
    enum : u8 {
      jrnl_start = 1,     // Start transaction block
      jrnl_data,          // Data block
      jrnl_commit,        // Commit transaction block
    };

    // Keeps track of free and allocated blocks on the disk. Transactions that
    // modify the block free bitmap on the disk go through a list of free_bit
    // structs in memory first.
    typedef struct free_bit {
      u32 bno_;
      bool is_free;
      sleeplock write_lock;
      ilink<free_bit> link;

      free_bit(u32 bno, bool f): bno_(bno), is_free(f) {}
      NEW_DELETE_OPS(free_bit);

      free_bit& operator=(const free_bit&) = delete;
      free_bit(const free_bit&) = delete;

      free_bit& operator=(free_bit&&) = default;
      free_bit(free_bit&&) = default;
    } free_bit;

    NEW_DELETE_OPS(mfs_interface);
    mfs_interface();

    void free_inode(u64 mnode_mnum, transaction *tr);
    // File functions
    u64 get_file_size(u64 mfile_mnum);
    void update_file_size(u64 mfile_mnum, u32 size, transaction *tr);
    void initialize_file(sref<mnode> m);
    int load_file_page(u64 mfile_mnum, char *p, size_t pos, size_t nbytes);
    int sync_file_page(u64 mfile_mnum, char *p, size_t pos, size_t nbytes,
                       transaction *tr);
    sref<inode> alloc_inode_for_mnode(u64 mnum, u8 type);
    u64 create_file_dir_if_new(u64 mnum, u64 parent_mnum, u8 type,
                               transaction *tr);
    void truncate_file(u64 mfile_mnum, u32 offset, transaction *tr);

    // Directory functions
    void initialize_dir(sref<mnode> m);
    void create_directory_entry(u64 mdir_mnum, char *name, u64 dirent_mnum,
                                u8 type, transaction *tr);
    void unlink_old_inode(u64 mdir_mnum, char* name, transaction *tr);
    void delete_old_inode(u64 mfile_mnum, transaction *tr);

    bool inum_lookup(u64 mnum, u64 *inum);

    // Initializes the root directory. Called during boot.
    sref<mnode> load_root();

    // Journal functions
    void add_to_journal_locked(transaction *tr);
    void pre_process_transaction(transaction *tr);
    void post_process_transaction(transaction *tr);
    void apply_trans_on_disk(transaction *tr);
    void add_fsync_to_journal(transaction *tr, bool flush_journal);
    void flush_journal_locked();
    void write_journal_hdrblock(const char *header, const char *datablock,
                                transaction *tr);
    void write_journal_header(u8 hdr_type, u64 timestamp, transaction *tr);
    bool fits_in_journal(size_t num_trans_blocks);
    void write_journal_trans_prolog(u64 timestamp, transaction *tr);
    void write_journal_transaction_blocks(const
    std::vector<std::unique_ptr<transaction_diskblock> >& vec, const u64 timestamp,
    transaction *tr);
    void write_journal_trans_epilog(u64 timestamp, transaction *tr);
    void process_journal();
    void clear_journal();

    // Metadata functions
    void metadata_log_alloc(u64 mnum);
    void metadata_op_start(u64 mnum, size_t cpu, u64 tsc_val);
    void metadata_op_end(u64 mnum, size_t cpu, u64 tsc_val);
    void add_to_metadata_log(u64 mnum, mfs_operation *op);
    void sync_dirty_files();
    void evict_bufcache();
    void evict_pagecache();
    void process_metadata_log();
    void process_metadata_log_and_flush();
    void process_metadata_log(u64 max_tsc, u64 mnode_mnum, bool isdir);
    void process_metadata_log_and_flush(u64 max_tsc, u64 mnum, bool isdir);
    void find_dependent_mnodes(mfs_logical_log *mfs_log, u64 mnode_mnum,
                               mfs_operation_vec &ops,
                               std::vector<u64> &dependent_mnodes, bool isdir);
    void mfs_create(mfs_operation_create *op, transaction *tr);
    void mfs_link(mfs_operation_link *op, transaction *tr);
    void mfs_unlink(mfs_operation_unlink *op, transaction *tr);
    void mfs_delete(mfs_operation_delete *op, transaction *tr);
    void mfs_rename(mfs_operation_rename *op, transaction *tr);

    // Block free bit vector functions
    void initialize_free_bit_vector();
    u32  alloc_block();
    void free_block(u32 bno);
    void print_free_blocks(print_stream *s);

    void preload_oplog();

  private:
    void load_dir(sref<inode> i, sref<mnode> m);
    sref<mnode> load_dir_entry(u64 inum, sref<mnode> parent);
    sref<mnode> mnode_alloc(u64 inum, u8 mtype);
    sref<inode> get_inode(u64 mnum, const char *str);
    // Mapping from disk inode numbers to the corresponding mnodes
    chainhash<u64, sref<mnode>> *inum_to_mnode;
    // Mapping from in-memory mnode numbers to disk inode numbers
    chainhash<u64, u64> *mnum_to_inum;

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

    journal *fs_journal;            // The phsyical journal
    chainhash<u64, mfs_logical_log*> *metadata_log_htab; // The logical log
    sref<inode> sv6_journal;

    // The free block bitmap in memory. Transactions marking a block free or not
    // free on the bitmap on disk make the corresponding changes in this vector
    // first. (Essential for reverting changes in case an fsync(file) fails.)
    std::vector<free_bit*> free_bit_vector;

    // A linked-list of bits (in the free_bit_vector) that are actually free.
    // Used to speed up block allocation and make it O(1).
    ilist<free_bit, &free_bit::link> free_bit_freelist;
    sleeplock freelist_lock; // Synchronizes access to free_bit_freelist.
};

class mfs_operation
{
  public:
    NEW_DELETE_OPS(mfs_operation);

    mfs_operation(mfs_interface *p, u64 t): parent_mfs(p), timestamp(t)
    {
      assert(parent_mfs);
    }

    virtual ~mfs_operation() {}
    virtual void apply(transaction *tr) = 0;
    virtual void print() = 0;

    // If the mfs_operation depends on other mnodes (such as the parent mnode),
    // those mnodes are added to dependent_mnodes.
    virtual bool check_dependency(std::vector<u64> &dependent_mnodes) = 0;

    virtual bool check_parent_dependency(std::vector<u64> &mnode_vec, u64 pt) = 0;

  protected:
    mfs_interface *parent_mfs;
  public:
    const u64 timestamp;
};

class mfs_operation_create: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_create);

    mfs_operation_create(mfs_interface *p, u64 t, u64 mnum, u64 pt, char nm[],
                         short m_type)
      : mfs_operation(p, t), mnode_mnum(mnum), parent_mnum(pt), mnode_type(m_type)
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

    bool check_dependency (std::vector<u64> &dependent_mnodes) override
    {
      for (auto it = dependent_mnodes.begin(); it != dependent_mnodes.end();
           it++) {
        if (*it == parent_mnum)
          return true;
      }
      dependent_mnodes.push_back(parent_mnum);
      return true;
    }

    // TODO: Revisit this later, when handling fsync() on directories.
    bool check_parent_dependency(std::vector<u64> &mnode_vec, u64 pt)
    {
      if (parent_mnum == pt) {
        for (auto it = mnode_vec.begin(); it != mnode_vec.end(); it++)
          if (*it == mnode_mnum)
            return true;
        mnode_vec.push_back(mnode_mnum);
        return true;
      }
      return false;
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
      : mfs_operation(p, t), mnode_mnum(mnum), parent_mnum(pt), mnode_type(m_type)
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

    bool check_dependency (std::vector<u64> &dependent_mnodes) override
    {
      for (auto it = dependent_mnodes.begin(); it != dependent_mnodes.end();
           it++) {
        if (*it == parent_mnum)
          return true;
      }
      dependent_mnodes.push_back(parent_mnum);
      return true;
    }

    // TODO: Revisit this later, when handling fsync() on directories.
    bool check_parent_dependency(std::vector<u64> &mnode_vec, u64 pt)
    {
      if (parent_mnum == pt) {
        for (auto it = mnode_vec.begin(); it != mnode_vec.end(); it++)
          if (*it == mnode_mnum)
            return true;
        mnode_vec.push_back(mnode_mnum);
        return true;
      }
      return false;
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

    mfs_operation_unlink(mfs_interface *p, u64 t, u64 mnum, u64 pt, char nm[])
      : mfs_operation(p, t), mnode_mnum(mnum), parent_mnum(pt)
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

    bool check_dependency (std::vector<u64> &dependent_mnodes) override
    {
      // The corresponding create or link operation would have already
      // handled the dependencies (if any).
      return true;
    }

    // TODO: Revisit this later, when handling fsync() on directories.
    bool check_parent_dependency(std::vector<u64> &mnode_vec, u64 pt)
    {
      if (parent_mnum == pt) {
        for (auto it = mnode_vec.begin(); it != mnode_vec.end(); it++)
          if (*it == mnode_mnum)
            return true;
        mnode_vec.push_back(mnode_mnum);
        return true;
      }
      return false;
    }

    void print()
    {
      cprintf("UNLINK\n");
      cprintf("Op Type : Unlink\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
      cprintf("Parent Mnode Num: %ld\n", parent_mnum);
      cprintf("Name: %s\n", name);
    }

  private:
    u64 mnode_mnum;   // mnode number of the file/directory to be unlinked
    u64 parent_mnum;  // mnode number of the parent directory
    char *name;       // name of the file/directory
};


// This operation is used to delete an inode and its file-contents, when its
// last link has been removed and its last open file descriptor has been closed.
// Called when the corresponding mnode's reference count drops to zero (which
// indicates that both the above conditions are true).

// Special, invalid mnode number, used to consolidate all delete operations
// together in a common, dedicated (per-cpu) mfs_log instance.
#define MFS_DELETE_MNUM 0

class mfs_operation_delete: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_delete);

    mfs_operation_delete(mfs_interface *p, u64 t, u64 mnum)
      : mfs_operation(p, t), mnode_mnum(mnum) { }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_delete(this, tr);
    }

    bool check_dependency(std::vector<u64> &dependent_mnodes) override
    {
      return true;
    }

    // TODO: Revisit this later, when handling fsync() on directories.
    bool check_parent_dependency(std::vector<u64> &mnode_vec, u64 pt)
    {
      return false;
    }

    void print()
    {
      cprintf("DELETE\n");
      cprintf("Op Type : Delete\n");
      cprintf("Timestamp: %ld\n", timestamp);
      cprintf("Mnode Num: %ld\n", mnode_mnum);
    }

  private:
    u64 mnode_mnum;  // mnode number of the file/directory to be deleted
};

class mfs_operation_rename: public mfs_operation
{
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(mfs_operation_rename);

    mfs_operation_rename(mfs_interface *p, u64 t, char oldnm[], u64 mnum,
                         u64 src_pt, char newnm[], u64 dst_pt, u8 m_type)
      : mfs_operation(p, t), mnode_mnum(mnum), src_parent_mnum(src_pt),
        dst_parent_mnum(dst_pt), mnode_type(m_type)
    {
      name = new char[DIRSIZ];
      newname = new char[DIRSIZ];
      strncpy(name, oldnm, DIRSIZ);
      strncpy(newname, newnm, DIRSIZ);
    }

    ~mfs_operation_rename()
    {
      delete[] name;
      delete[] newname;
    }

    void apply(transaction *tr) override
    {
      parent_mfs->mfs_rename(this, tr);
    }

    bool check_dependency (std::vector<u64> &dependent_mnodes) override
    {
      // The corresponding create or link of the mnode would have already
      // added the source directory to the dependency list. So we just need
      // to add the destination directory here.

      for (auto it = dependent_mnodes.begin(); it != dependent_mnodes.end();
           it++) {
        if (*it == dst_parent_mnum)
          return true;
      }
      dependent_mnodes.push_back(dst_parent_mnum);
      return true;
    }

    // TODO: Revisit this later, when handling fsync() on directories.
    bool check_parent_dependency(std::vector<u64> &mnode_vec, u64 pt)
    {
      if (src_parent_mnum == pt || dst_parent_mnum == pt) {
        bool is_parent = false;
        if (src_parent_mnum == pt)
          is_parent = true;

        bool present = false;
        for (auto it = mnode_vec.begin(); it != mnode_vec.end(); it++) {
          if (*it == mnode_mnum) {
            present = true;
            break;
          }
        }
        if (!present)
          mnode_vec.push_back(mnode_mnum);

        present = false;
        for (auto it = mnode_vec.begin(); it != mnode_vec.end(); it++) {
          if (is_parent && *it == dst_parent_mnum) {
            present = true;
            break;
          } else if (!is_parent && *it == src_parent_mnum) {
            present = true;
            break;
          }
        }

        if (!present) {
          if (is_parent)
            mnode_vec.push_back(dst_parent_mnum);
          else
            mnode_vec.push_back(src_parent_mnum);
        }
        return true;
      }
      return false;
    }

    void print()
    {
      cprintf("RENAME\n");
      cprintf("Op Type : Rename\n");
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

// The "logical" log of metadata operations. These operations are applied on an fsync
// call so that any previous dependencies can be resolved before the mnode is fsynced.
// The list of operations is oplog-maintained.
class mfs_logical_log: public mfs_logged_object
{
  friend mfs_interface;

  public:
    NEW_DELETE_OPS(mfs_logical_log);
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
        parent->operation_vec.push_back(std::move(operation));
      }

      void print()
      {
        operation->print();
      }

      u64 get_tsc()
      {
        return operation->timestamp;
      }

      private:
      mfs_logical_log *parent;
      mfs_operation *operation;
    };

    void add_operation(mfs_operation *op)
    {
      get_logger()->push_with_tsc<add_op>(add_op(this, op));
    }

    // This function pre-allocates a per-core logger for this core,
    // and sets the bit corresponding to this CPU in oplog's bitmap.
    // This helps avoid extraneous sharing reports when using commuter.
    void preload_oplog()
    {
      get_logger();
    }

  protected:
    mfs_operation_vec operation_vec;
    sleeplock lock;
};
