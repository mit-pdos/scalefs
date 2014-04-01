#pragma once

#include "linearhash.hh"
#include "buf.hh"
#include <vector>

class mnode;

typedef struct transaction_diskblock {
  u32 blocknum;
  char blockdata[512];

  transaction_diskblock(u32 n, char buf[512]) {
    blocknum = n;
    memmove(blockdata, buf, 512);
  }

  transaction_diskblock(u32 n) {
    blocknum = n;
    memset(blockdata, 0, 512);
  }
}transaction_diskblock;

class transaction {
  public:
    NEW_DELETE_OPS(transaction);
    transaction() {
      blocks = std::vector<transaction_diskblock>();
    }
    void add_block(transaction_diskblock b) {
      blocks.push_back(b);
    }
    void commit_transaction() {
      sref<buf> bp;
      for (int i = 0; i < blocks.size(); i++) {
        bp = buf::get(1, blocks.at(i).blocknum);
        if (bp->dirty())
          bp->writeback();
      }
    }
  
  private:
    std::vector<transaction_diskblock> blocks;
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
      for (int i = 0; i < transaction_log.size(); i++) {
        transaction_log.at(i)->commit_transaction();
        delete transaction_log.at(i);
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
    u64 get_file_size(u64 mnode_inum);
    void initialize_file(sref<mnode> m); 
    int load_file_page(u64 mnode_inum, char *p, size_t pos, size_t nbytes);
    int sync_file_page(u64 mnode_inum, char *p, size_t pos, size_t nbytes,
                              transaction *tr);
    void create_file_if_new(u64 mnode_inum, u8 type, transaction *tr);
    void truncate_file(u64 mnode_inum, transaction *tr);
    void initialize_dir(sref<mnode> m);
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

