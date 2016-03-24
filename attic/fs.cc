
// Unused code, but potentially useful (at least as a reference).

u64
ino_hash(const pair<u32, u32> &p)
{
  return p.first ^ p.second;
}

static nstbl<pair<u32, u32>, inode*, ino_hash> *ins;

void
dir_flush(sref<inode> dp, transaction *trans)
{
  // assume already locked
  //cprintf("Calling dir_flush on dp with inum %d\n", dp->inum);
  if (!dp->dir)
    return;

  u32 off = 0;
  char *buffer = (char *)zalloc("dir_flush");

  dp->dir.load()->enumerate([&dp, &off, trans, buffer](const strbuf<DIRSIZ> &name, const u32 &inum)->bool{
      struct dirent de;
      strncpy(de.name, name.buf_, DIRSIZ);
      de.inum = inum;

      void *buf = buffer + off;
      const void *de_ptr = &de;
      memmove(buf, de_ptr, sizeof(de));
      off += sizeof(de);

      if (off > PGSIZE)
        panic("dir_flush buffer overflow");

      return false;
    });

  if (writei(dp, buffer, 0, PGSIZE, trans) != PGSIZE)
    panic("dir_flush writei");

  if (dp->size != off) {
    auto w = dp->seq.write_begin();
    dp->size = off;
  }
  iupdate(dp, trans);
}

void
dir_remove_entries(sref<inode> dp, std::vector<char*> names_vec) {
  dir_init(dp);
  dp->dir.load()->enumerate([&names_vec, &dp](const strbuf<DIRSIZ> &name, const u32 &inum)->bool{
      bool exists = false;
      for (auto it = names_vec.begin(); it != names_vec.end(); it++) {
        if (strcmp(*it, name.buf_) == 0) {
          exists = true;
          break;
        }
      }
      if (exists) {
        sref<inode> ip = iget(dp->dev, inum);
        if (ip->type == T_DIR)
          dirunlink(dp, name.buf_, inum, true);
        else if (ip->type == T_FILE)
          dirunlink(dp, name.buf_, inum, false);
      }
      return false;
    });
}

void
dir_remove_entry(sref<inode> dp, char* entry_name) {
  dir_init(dp);
  dp->dir.load()->enumerate([&entry_name, &dp](const strbuf<DIRSIZ> &name, const u32 &inum)->bool{
      if (strcmp(entry_name, name.buf_) == 0) {
        sref<inode> ip = iget(dp->dev, inum);
        if (ip->type == T_DIR)
          dirunlink(dp, name.buf_, inum, true);
        else if (ip->type == T_FILE)
          dirunlink(dp, name.buf_, inum, false);
      }
      return false;
    });
}
