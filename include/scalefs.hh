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
typedef struct transaction_diskblock transaction_diskblock;
typedef struct mfs_operation mfs_operation;
typedef std::vector<mfs_operation*> mfs_operation_vec;
typedef std::vector<mfs_operation*>::iterator mfs_operation_iterator;

static u64 get_timestamp() {
  if (cpuid::features().rdtscp)
    return rdtscp();
  // cpuid + rdtsc combination (more expensive) is used as an alternative to
  // rdtscp where it's not supported.
  return rdtsc_serialized();
}

// A single disk block that was updated as the result of a transaction. All
// diskblocks that were written to during the transaction are stored as a linked
// list in the transaction object.
struct transaction_diskblock {
  u32 blocknum;           // The disk block number
  char blockdata[BSIZE];  // Disk block contents
  u64 timestamp;          // Updates within a transaction should be written out
                          // in timestamp order. A single disk block might have
                          // been updated several times, but the changes not
                          // necessarily logged in timestamp order.

  transaction_diskblock(u32 n, char buf[BSIZE]) {
    blocknum = n;
    memmove(blockdata, buf, BSIZE);
    timestamp = get_timestamp();
  }

  // A disk block that has been zeroed out.
  transaction_diskblock(u32 n) {
    blocknum = n;
    memset(blockdata, 0, BSIZE);
    timestamp = get_timestamp();
  }

  // Write out the block contents to disk block # blocknum.
  void writeback() {
    sref<buf> bp = buf::get(1, blocknum);
    auto cmp = bp->read();
    if (strcmp(cmp->data, blockdata) != 0)
      cprintf("%ld: Bufcache and transaction diskblock %d don't match\n",
        timestamp, blocknum);
    idewrite(1, blockdata, BSIZE, blocknum*BSIZE);
  }

  void writeback_through_bufcache() {
    sref<buf> bp = buf::get(1, blocknum);
    if (bp->dirty())
      bp->writeback();
  }

};

// A transaction represents all related updates that take place as the result of a
// filesystem operation.
class transaction {
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(transaction);
    transaction() : timestamp_(get_timestamp()) {
      blocks = std::vector<transaction_diskblock>();
    }

    // Add a diskblock to the transaction. These diskblocks are not necessarily
    // added in timestamp order. They should be sorted before actually flushing
    // out the changes to disk.
    void add_block(transaction_diskblock b) {
      auto l = write_lock.guard();
      blocks.push_back(b);
    }

    // Prepare the transaction for two phase commit. The transaction diskblocks
    // are ordered by timestamp. (This is needed to ensure that mutliple changes
    // to the same block are written to disk in the correct order.)
    void prepare_for_commit() {
      // All relevant blocks must have been added to the transaction at
      // this point. A try acquire must succeed.
      auto l = write_lock.try_guard();
      assert(static_cast<bool>(l));

      // Sort the diskblocks in timestamp order.
      std::sort(blocks.begin(), blocks.end(), compare_timestamp_db);
    }
    
    // Write the blocks in this transaction to disk. Used to write the journal.
    void write_to_disk() {
      for (auto b = blocks.begin(); b != blocks.end(); b++)
        b->writeback();
      blocks.clear();
    }

    // comparison function to order diskblock updates in timestamp order
    static bool compare_timestamp_db(transaction_diskblock b1, transaction_diskblock b2) {
      return (b1.timestamp < b2.timestamp);
    }

    // Transactions need to be applied in timestamp order too. They might not
    // have been logged in the journal in timestamp order.
    const u64 timestamp_;

  private:
    // List of updated diskblocks
    std::vector<transaction_diskblock> blocks;

    // Guards updates to the transaction_diskblock vector.
    spinlock write_lock;
};

// The "physical" journal is made up of transactions, which in turn are made up of
// updated diskblocks.
class journal {
  friend mfs_interface;
  public:
    NEW_DELETE_OPS(journal);
    journal() {
      transaction_log = std::vector<transaction*>();
    }

    // Add a new transaction to the journal.
    void add_transaction(transaction *tr) {
      // File system operations are serialized at this lock. Is this really a
      // scalability hit, given that transactions are only added on a call to
      // fsync(). Is it reasonable to slow down during an fsync()?
      auto l = write_lock.guard();
      transaction_log.push_back(tr);
    }

    lock_guard<spinlock> prepare_for_commit() {
      auto l = write_lock.guard();

      // Sort the transactions in timestamp order.
      std::sort(transaction_log.begin(), transaction_log.end(), compare_timestamp_tr);

      for (auto it = transaction_log.begin(); it != transaction_log.end(); it++)
        (*it)->prepare_for_commit();

      return l;
    }

    // comparison function to order journal transactions in timestamp order
    static bool compare_timestamp_tr(transaction *t1, transaction *t2) {
      return (t1->timestamp_ < t2->timestamp_);
    }

  private:
    // List of transactions (Unordered).
    std::vector<transaction*> transaction_log;
    // Guards updates to the transaction_log
    spinlock write_lock;
};

// Struct mfs_operation represents each filesystem metadata operation
struct mfs_operation {
  int op_type;      // type of the metadata operation
  u64 mnode;        // mnode number on which the operation takes place
  u64 parent;       // mnode number of the parent
  u64 new_mnode;    // used for rename operation
  u64 new_parent;   // used for rename operation
  short mnode_type; // creation type for the new inode
  char *name;       // name of the file/directory
  char *newname;    // used for rename operation

  // Types of metadata operations
  enum {
    op_create,
    op_link,
    op_unlink,
    op_rename,
    op_truncate
  };

  mfs_operation(int type, u64 mn, u64 pt, char nm[], short m_type = 0)
    : op_type(type), mnode(mn), parent(pt), mnode_type(m_type), newname(NULL) {
      name = new char[DIRSIZ];
      strncpy(name, nm, DIRSIZ);
    }   

  mfs_operation(int type, u64 mn, u32 st, u32 nb) 
    : op_type(type), mnode(mn), name(NULL), newname(NULL) {}   

  mfs_operation(int type, char oldnm[], u64 mn, u64 pt, 
      char newnm[], u64 newmn, u64 newpt, u8 m_type)
    : op_type(type), mnode(mn), parent(pt), new_mnode(newmn),
    new_parent(newpt), mnode_type(m_type) {
      name = new char[DIRSIZ];
      newname = new char[DIRSIZ];
      strncpy(name, oldnm, DIRSIZ);
      strncpy(newname, newnm, DIRSIZ);
    }   

  mfs_operation(int type, u64 mn) 
    : op_type(type), mnode(mn), name(NULL), newname(NULL) {}   

  ~mfs_operation() {
    if (name)
      delete[] name;
    if (newname)
      delete[] newname;
  }

  NEW_DELETE_OPS(mfs_operation);

  void print_operation() {
    cprintf("Op Type : %d\n", op_type);
    cprintf("Mnode: %ld\n", mnode);
    cprintf("Parent: %ld\n", parent);
    cprintf("New mnode: %ld\n", new_mnode);
    cprintf("New parent: %ld\n", new_parent);
    cprintf("Mnode type: %d\n", mnode_type);
    if (name)
      cprintf("Name: %s\n", name);
    if (newname)
      cprintf("Newname: %s\n", newname);
  }
};

// The "logical" log of metadata operations. These operations are applied on an fsync 
// call so that any previous dependencies can be resolved before the mnode is fsynced.
// The list of operations is oplog-maintained.
class mfs_logical_log: public tsc_logged_object {
  friend mfs_interface;

  public:
    NEW_DELETE_OPS(mfs_logical_log);
  
    // Oplog operation that implements adding a metadata operation to the log
    struct add_op {
      add_op(mfs_logical_log *l, mfs_operation *op): parent(l), operation(op) {
        assert(l);
      }
      void operator()() {
        parent->operation_vec.push_back(std::move(operation));
      }
      void print() {
        operation->print_operation();
      }

      private:
      mfs_logical_log *parent;
      mfs_operation *operation;
    };

    void add_operation(mfs_operation *op) {
      get_logger()->push<add_op>(add_op(this, op));
    }

  protected:
    mfs_operation_vec operation_vec;
};

// This class acts as an interfacing layer between the in-memory representation
// of the filesystem and the on-disk representation. It provides functions to
// convert mnode operations to inode operations and vice versa. This also
// functions as a container for the physical and logical logs.
class mfs_interface {
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

    NEW_DELETE_OPS(mfs_interface);
    mfs_interface();

    // File functions
    u64 get_file_size(u64 mfile_inum);
    void update_file_size(u64 mfile_inum, u32 size, transaction *tr);
    void initialize_file(sref<mnode> m); 
    int load_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes);
    int sync_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes,
                              transaction *tr);
    u64 create_file_if_new(u64 mfile_inum, u64 parent, u8 type, char *name,
          transaction *tr, bool sync_parent = true);
    void truncate_file(u64 mfile_inum, u32 offset, transaction *tr);

    // Directory functions
    void initialize_dir(sref<mnode> m);
    u64 create_dir_if_new(u64 mdir_inum, u64 parent, u8 type, char *name, 
          transaction *tr, bool sync_parent = true);
    void create_directory_entry(u64 mdir_inum, char *name, u64 dirent_inum,
          u8 type, transaction *tr);
    void update_dir_inode(u64 mdir_inum, transaction *tr);
    void unlink_old_inodes(u64 mdir_inum, std::vector<char*> names_vec, 
          transaction *tr); 

    void create_mapping(u64 mnode, u64 inode);
    bool inode_lookup(u64 mnode, u64 *inum);

    // Initializes the root directory. Called during boot.
    sref<mnode> load_root();

    // Adds a transaction to the physical journal.
    void add_to_journal(transaction *tr) {
      fs_journal->add_transaction(tr);
    }

    // Writes out the physical journal to the disk. Then applies the
    // flushed out transactions to the disk filesystem.
    void flush_journal() {
      auto journal_lock = fs_journal->prepare_for_commit();

      for (auto it = fs_journal->transaction_log.begin(); 
        it != fs_journal->transaction_log.end(); it++) {
        // Write out the transaction blocks to the disk journal in timestamp order. 
        write_transaction_to_journal((*it)->blocks, (*it)->timestamp_);

        // This transaction has been written to the journal. Writeback the changes 
        // to original location on disk. 
        for (auto b = (*it)->blocks.begin(); b != (*it)->blocks.end(); b++)
          b->writeback();
        (*it)->blocks.clear();
        delete (*it);
        // The blocks have been written to disk successfully. Safe to delete
        // this transaction from the journal. (This means that all the
        // transactions till this point have made it to the disk. So the journal
        // can simply be truncated.)
        clear_journal();
      }
      fs_journal->transaction_log.clear();
    }

    // Writes out a single journal transaction to disk
    void write_transaction_to_journal(const std::vector<transaction_diskblock> vec, 
          const u64 timestamp);

    // Called on reboot after a crash. Applies committed transactions.
    void process_journal();

    // Truncate the journal on the disk
    void clear_journal();

    // Adds a metadata operation to the logical log.
    void add_to_metadata_log(mfs_operation *op) {
      metadata_log->add_operation(op);
    }

    // Applies metadata operations logged in the logical journal. Called on
    // fsync to resolve any metadata dependencies.
    void process_metadata_log() {
      // Synchronize the oplog loggers.
      auto guard = metadata_log->synchronize();

      for (auto it = metadata_log->operation_vec.begin(); 
          it != metadata_log->operation_vec.end(); it++) {
        transaction *tr = new transaction();
        apply_metadata_operation(*it, tr);
        add_to_journal(tr);
        delete (*it);
      }
      metadata_log->operation_vec.clear();
    }

    // Metadata functions
    void apply_metadata_operation(mfs_operation *op, transaction *tr);
    void mfs_create(mfs_operation *op, transaction *tr);
    void mfs_link(mfs_operation *op, transaction *tr);
    void mfs_unlink(mfs_operation *op, transaction *tr);
    void mfs_rename(mfs_operation *op, transaction *tr);
    void mfs_truncate(mfs_operation *op, transaction *tr);

  private:
    void load_dir(sref<inode> i, sref<mnode> m); 
    sref<mnode> load_dir_entry(u64 inum, sref<mnode> parent);
    sref<mnode> mnode_alloc(u64 inum, u8 mtype);
    sref<inode> get_inode(u64 mnode_inum, const char *str);

    // Mapping from disk inode numbers to the corresponding mnodes
    linearhash<u64, sref<mnode>> *inum_to_mnode;

    // Mapping from in-memory mnode numbers to disk inode numbers
    linearhash<u64, u64> *mnode_to_inode;
    
    journal *fs_journal;            // The phsyical journal
    mfs_logical_log *metadata_log;  // The logical log

};
