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
