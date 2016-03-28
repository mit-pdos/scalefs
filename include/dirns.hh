#pragma once

#include "nstbl.hh"

struct dir_entry_info {
  u32 inum_;
  u32 offset_;

  dir_entry_info() : inum_(0), offset_(0) {}
  dir_entry_info(u32 inum, u32 offset) : inum_(inum), offset_(offset) {}
  NEW_DELETE_OPS(dir_entry_info);

  bool operator==(const dir_entry_info &o) const
  {
    return inum_ == o.inum_ && offset_ == o.offset_;
  }

  bool operator!=(const dir_entry_info &o) const
  {
    return inum_ != o.inum_ || offset_ != o.offset_;
  }
};

u64 namehash(const strbuf<DIRSIZ>&);
class dirns : public nstbl<strbuf<DIRSIZ>, dir_entry_info, namehash> {};

class dir_entries {
public:
  dir_entries(u64 size) : map_(size) {}
  NEW_DELETE_OPS(dir_entries);

  bool lookup(const strbuf<DIRSIZ>& name, dir_entry_info *de_info_ptr)
  {
    return map_.lookup(name, de_info_ptr);
  }

  bool insert(const strbuf<DIRSIZ>& name, const dir_entry_info& de_info)
  {
    return map_.insert(name, de_info);
  }

  bool remove(const strbuf<DIRSIZ>& name)
  {
    return map_.remove(name);
  }

private:
  chainhash<strbuf<DIRSIZ>, dir_entry_info> map_;
};
