#pragma once

#include "linearhash.hh"
#include "buf.hh"
#include "cpuid.hh"
#include <vector>

class mnode;

static u64 get_timestamp() {
  if (cpuid::features().rdtscp)
    return rdtscp();
  return rdtsc_serialized();
}

typedef struct transaction_diskblock {
  u32 blocknum;
  char blockdata[BSIZE];
  u64 timestamp;

  transaction_diskblock(u32 n, char buf[BSIZE]) {
    blocknum = n;
    memmove(blockdata, buf, BSIZE);
    timestamp = get_timestamp();
  }

  transaction_diskblock(u32 n) {
    blocknum = n;
    memset(blockdata, 0, BSIZE);
    timestamp = get_timestamp();
  }
}transaction_diskblock;

class transaction {
  public:
    NEW_DELETE_OPS(transaction);
    transaction() : timestamp_(get_timestamp()) {
      blocks = std::vector<transaction_diskblock>();
    }

    void add_block(transaction_diskblock b) {
      auto l = write_lock.guard();
      blocks.push_back(b);
    }

    void commit_transaction() {
      // XXX Write the transaction out to disk before carrying out the
      // corresponding filesystem operations (Two-phase commit)

      // All relevant blocks must have been added to the transaction at
      // this point. A try acquire must succeed.
      auto l = write_lock.try_guard();
      assert(static_cast<bool>(l));

      sref<buf> bp;
      for (auto it = blocks.begin(); it != blocks.end(); it++) {
        bp = buf::get(1, it->blocknum);
        if (bp->dirty())
          bp->writeback();
      }
    }

    void log_new_file(u64 inum) {
      auto l = write_lock.guard();
      new_files.push_back(inum);
    }

    const u64 timestamp_;
  
  private:
    std::vector<transaction_diskblock> blocks;
    std::vector<u64> new_files;
    spinlock write_lock;
};

class journal {
  public:
    NEW_DELETE_OPS(journal);
    journal() {
      transaction_log = std::vector<transaction*>();
    }

    void add_transaction(transaction *tr) {
      transaction_log.push_back(tr);
    }

    void flush_to_disk() {
      for (auto it = transaction_log.begin(); it != transaction_log.end(); it++) {
        (*it)->commit_transaction();
        delete (*it);
      }
      transaction_log.clear();
    }

  private:
    std::vector<transaction*> transaction_log;
};

class mfs_interface {
  public:
    NEW_DELETE_OPS(mfs_interface);
    mfs_interface();
    u64 get_file_size(u64 mfile_inum);
    void update_file_size(u64 mfile_inum, u32 size, transaction *tr);
    void initialize_file(sref<mnode> m); 
    int load_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes);
    int sync_file_page(u64 mfile_inum, char *p, size_t pos, size_t nbytes,
                              transaction *tr);
    u64 create_file_if_new(u64 mfile_inum, u64 parent, u8 type, char *name,
          transaction *tr, bool sync_parent = false);
    void truncate_file(u64 mfile_inum, u32 offset, transaction *tr);
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
    sref<mnode> load_root();

    void add_to_journal(transaction *tr) {
      fs_journal->add_transaction(tr);
    }
    void flush_journal() {
      fs_journal->flush_to_disk();
    }

  private:
    void load_dir(sref<inode> i, sref<mnode> m); 
    sref<mnode> load_dir_entry(u64 inum);
    sref<mnode> mnode_alloc(u64 inum, u8 mtype);
    sref<inode> get_inode(u64 mnode_inum, const char *str);

    linearhash<u64, sref<mnode>> *inum_to_mnode;
    linearhash<u64, u64> *mnode_to_inode;
    journal *fs_journal;
};

