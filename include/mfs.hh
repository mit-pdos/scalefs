#pragma once

#include "mnode.hh"
#include "spinlock.hh"
#include "linearhash.hh"

/* The log of file system operations. Currently this is global. Will support per-core later. */
extern fs_sync_op *fs_log;

/* The lock protecting fs_log. Currently all operations are serialized at this lock. */
extern spinlock fs_log_lock;

extern fssync();

/* Struct fs_sync_op represents each entry in the file system operations log */
struct fs_sync_op {
  int op_type;      // the type of file system operation
  u64 mnode;        // mnode number on which the operation takes place
  u64 parent;       // mnode number of the parent
  u64 new_mnode;    // used for move operation
  u64 new_parent;   // used for move operation
  char *buf;        // data to be written (file write)
  u32 start;        // start offset for file write
  u32 nbytes;       // number of bytes to be written (file write)
  short create_type;  // creation type for new inode
  char *name;  // name of the file/directory
  char *newname; // used for move operation
  fs_sync_op *next;    // pointer to the next file system operation

  fs_sync_op(int type, u64 mn, u64 pt, char nm[], short c_type = 0)
    : op_type(type), mnode(mn), parent(pt), buf(NULL), 
        create_type(c_type), newname(NULL), next(NULL) {
      name = new char[DIRSIZ];
      strncpy(name, nm, DIRSIZ);
    }

  fs_sync_op(int type, u64 mn, const char *str, u32 st, u32 nb)
    : op_type(type), mnode(mn), start(st), nbytes(nb),
        name(NULL), newname(NULL), next(NULL) {
      if (nbytes > 0) {
        buf = new char[nbytes];
        memset(buf, 0, nb);
        memcpy(buf, str, nb);
      } else {
        buf = NULL;
      }
    }

  fs_sync_op(int type, char oldnm[], u64 mn, u64 pt, 
              char newnm[], u64 newmn, u64 newpt, u8 c_type)
    : op_type(type), mnode(mn), parent(pt), new_mnode(newmn),
      new_parent(newpt), buf(NULL), create_type(c_type), next(NULL) {
      name = new char[DIRSIZ];
      newname = new char[DIRSIZ];
      strncpy(name, oldnm, DIRSIZ);
      strncpy(newname, newnm, DIRSIZ);
    }

  fs_sync_op(int type, u64 mn)
    : op_type(type), mnode(mn), buf(NULL), name(NULL), newname(NULL), next(NULL) {
    }

  ~fs_sync_op() {
    if (buf)
      delete[] buf;
    if (name)
      delete[] name;
    if (newname)
      delete[] newname;
  }

  NEW_DELETE_OPS(fs_sync_op);

  void log_insert() {
    static std::atomic<int> counter(0);
    counter++;
    fs_log_lock.acquire();
    if(!fs_log) {
      fs_log = this;
    } else {
      fs_sync_op *tmp = fs_log;
      while(tmp->next)
        tmp = tmp->next;
      tmp->next = this;
    }
    fs_log_lock.release();
    if (counter > 10) {
      fssync();
      counter = 0;
    }
  }

};

extern u64 root_inum;
extern mfs* root_fs;
extern mfs* anon_fs;
extern linearhash<u64, u64> *mnode_to_inode;

sref<mnode> namei(sref<mnode> cwd, const char* path);
sref<mnode> nameiparent(sref<mnode> cwd, const char* path, strbuf<DIRSIZ>* buf);
s64 readi(sref<mnode> m, char* buf, u64 start, u64 nbytes);
s64 writei(sref<mnode> m, const char* buf, u64 start, u64 nbytes,
           mfile::resizer* resize = nullptr);

class print_stream;
void mfsprint(print_stream *s);
