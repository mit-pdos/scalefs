
// Unused code, but potentially useful (at least as a reference).

u64
ino_hash(const pair<u32, u32> &p)
{
  return p.first ^ p.second;
}

static nstbl<pair<u32, u32>, inode*, ino_hash> *ins;

template<size_t N>
struct inode_cache;

template<size_t N>
struct inode_cache : public balance_pool<inode_cache<N>>
{
  inode_cache()
    : balance_pool<inode_cache<N>> (N),
      head_(0), length_(0), lock_("inode_cache", LOCKSTAT_FS)
  {
  }

  int
  alloc()
  {
    scoped_acquire _l(&lock_);
    return alloc_nolock();
  }

  void
  add(u32 inum)
  {
    scoped_acquire _l(&lock_);
    add_nolock(inum);
  }

  void
  balance_move_to(inode_cache<N>* target)
  {
    if (target < this) {
      target->lock_.acquire();
      lock_.acquire();
    } else {
      lock_.acquire();
      target->lock_.acquire();
    }

    u32 nmove = length_ / 2;
    for (; nmove; nmove--) {
      int inum = alloc_nolock();
      if (inum < 0) {
        console.println("inode_cache: unexpected failure");
        break;
      }
      target->add_nolock(inum);
    }

    if (target < this) {
      target->lock_.release();
      lock_.release();
    } else {
      lock_.release();
      target->lock_.release();
    }
  }

  u64
  balance_count() const
  {
    return length_;
  }

private:

  int
  alloc_nolock()
  {
    int inum = -1;
    if (length_) {
      length_--;
      head_--;
      inum = cache_[head_ % N];
    }
    return inum;
  }

  void
  add_nolock(u32 inum)
  {
    assert(inum != 0);
    if (length_ < N)
      length_++;
    cache_[head_ % N] = inum;
    head_++;
  }

  u32      cache_[N];
  u32      head_;
  u32      length_;
  spinlock lock_;
};

struct inode_cache_dir
{
  inode_cache_dir() : balancer_(this)
  {
  }

  inode_cache<512>*
  balance_get(int id) const
  {
    return &cache_[id];
  }

  void
  add(u32 inum)
  {
    // XXX(sbw) if cache->length_ == N should we call
    // balancer_.balance()?
    cache_->add(inum);
  }

  int
  alloc()
  {
    int inum = cache_->alloc();
    if (inum > 0)
      return inum;
    balancer_.balance();
    return cache_->alloc();
  }

private:

  percpu<inode_cache<512>, NO_CRITICAL> cache_;
  balancer<inode_cache_dir, inode_cache<512>> balancer_;
};

static inode_cache_dir the_inode_cache;


// Inode contents
//
// The contents (data) associated with each inode is stored
// in a sequence of blocks on the disk.  The first NDIRECT blocks
// are listed in ip->addrs[].  The next NINDIRECT blocks are
// listed in the block ip->addrs[NDIRECT].  The next NINDIRECT^2
// blocks are doubly-indirect from ip->addrs[NDIRECT+1].

// Return the disk block address of the nth block in inode ip.
// If there is no such block, bmap allocates one.
static u32
bmap(sref<inode> ip, u32 bn, transaction *trans = NULL, bool zero_on_alloc = false)
{
  scoped_gc_epoch e;

  u32* ap;
  u32 addr;

  if (bn < NDIRECT) {
  retry0:
    if ((addr = ip->addrs[bn]) == 0) {
      addr = balloc(ip->dev, trans, zero_on_alloc);
      if (!cmpxch(&ip->addrs[bn], (u32)0, addr)) {
        cprintf("bmap: race1\n");
        bfree(ip->dev, addr, trans);
        goto retry0;
      }
    }
    return addr;
  }
  bn -= NDIRECT;

  if (bn < NINDIRECT) {
  retry1:
    if (ip->iaddrs == nullptr) {
      if ((addr = ip->addrs[NDIRECT]) == 0) {
        addr = balloc(ip->dev, trans, true);
        if (!cmpxch(&ip->addrs[NDIRECT], (u32)0, addr)) {
          cprintf("bmap: race2\n");
          bfree(ip->dev, addr, trans);
          goto retry1;
        }
      }

      volatile u32* iaddrs = (u32*)kmalloc(IADDRSSZ, "iaddrs");
      sref<buf> bp = buf::get(ip->dev, addr);
      auto copy = bp->read();
      memmove((void*)iaddrs, copy->data, IADDRSSZ);

      if (!cmpxch(&ip->iaddrs, (volatile u32*)nullptr, iaddrs)) {
        kmfree((void*)iaddrs, IADDRSSZ);
        goto retry1;
      }
    }

  retry2:
    if ((addr = ip->iaddrs[bn]) == 0) {
      addr = balloc(ip->dev, trans, zero_on_alloc);
      if (!__sync_bool_compare_and_swap(&ip->iaddrs[bn], (u32)0, addr)) {
        cprintf("bmap: race4\n");
        bfree(ip->dev, addr, trans);
        goto retry2;
      }

      sref<buf> bp = buf::get(ip->dev, ip->addrs[NDIRECT]);
      auto locked = bp->write();
      ap = (u32 *)locked->data;
      ap[bn] = addr;
      if (trans)
        bp->add_to_transaction(trans);
    }

    return addr;
  }
  bn -= NINDIRECT;

  if (bn >= NINDIRECT * NINDIRECT)
    panic("bmap: %d out of range", bn);

  // Doubly-indirect blocks are currently "slower" because we do not
  // cache an equivalent of ip->iaddrs.

retry3:
  if (ip->addrs[NDIRECT+1] == 0) {
    addr = balloc(ip->dev, trans, true);
    if (!cmpxch(&ip->addrs[NDIRECT+1], (u32)0, addr)) {
      cprintf("bmap: race5\n");
      bfree(ip->dev, addr, trans);
      goto retry3;
    }
  }

  sref<buf> wb = buf::get(ip->dev, ip->addrs[NDIRECT+1]);

  for (;;) {
    auto copy = wb->read();
    ap = (u32*)copy->data;
    if (ap[bn / NINDIRECT] == 0) {
      auto locked = wb->write();
      ap = (u32*)locked->data;
      if (ap[bn / NINDIRECT] == 0) {
        ap[bn / NINDIRECT] = balloc(ip->dev, trans, true);
        if (trans)
          wb->add_to_transaction(trans);
      }
      continue;
    }
    addr = ap[bn / NINDIRECT];
    break;
  }

  wb = buf::get(ip->dev, addr);

  for (;;) {
    auto copy = wb->read();
    ap = (u32*)copy->data;
    if (ap[bn % NINDIRECT] == 0) {
      auto locked = wb->write();
      ap = (u32*)locked->data;
      if (ap[bn % NINDIRECT] == 0) {
        ap[bn % NINDIRECT] = balloc(ip->dev, trans, zero_on_alloc);
        if (trans)
          wb->add_to_transaction(trans);
      }
      continue;
    }
    addr = ap[bn % NINDIRECT];
    break;
  }

  return addr;
}

void
itrunc(sref<inode> ip, u32 offset, transaction *trans)
{
  scoped_gc_epoch e;

  if (ip->size <= offset)
    return;

  for (int i = BLOCKROUNDUP(offset); i < NDIRECT; i++) {
    if (ip->addrs[i]) {
      bfree(ip->dev, ip->addrs[i], trans, true);
      ip->addrs[i] = 0;
    }
  }

  if (ip->addrs[NDIRECT]) {
    int start = (offset >= NDIRECT*BSIZE) ?
      BLOCKROUNDUP(offset - NDIRECT*BSIZE) : 0;
    {
      sref<buf> bp = buf::get(ip->dev, ip->addrs[NDIRECT]);
      auto locked = bp->write();
      if (ip->iaddrs.load() != nullptr)
        memmove(locked->data, (void*)ip->iaddrs.load(), IADDRSSZ);

      u32* a = (u32*)locked->data;
      for (int i = start; i < NINDIRECT; i++) {
        if (a[i]) {
          bfree(ip->dev, a[i], trans, true);
          a[i] = 0;
        }
      }
      if (trans && start != 0)
        bp->add_to_transaction(trans);
    }

    if (start == 0) {
      bfree(ip->dev, ip->addrs[NDIRECT], trans, true);
      ip->addrs[NDIRECT] = 0;
    }
    if (ip->iaddrs.load() != nullptr) {
      kmfree((void*)ip->iaddrs.load(), IADDRSSZ);
      ip->iaddrs.store(nullptr);
    }
  }

  if (ip->addrs[NDIRECT+1]) {
    int bno = (offset >= (NDIRECT+NINDIRECT)*BSIZE)?
      BLOCKROUNDUP(offset-(NDIRECT+NINDIRECT)*BSIZE): 0;
    {
      sref<buf> bp1 = buf::get(ip->dev, ip->addrs[NDIRECT+1]);
      auto locked1 = bp1->write();
      u32* a1 = (u32*)locked1->data;
      for (int i = bno/NINDIRECT; i < NINDIRECT; i++) {
        if (!a1[i])
          continue;
        int start = (i == bno/NINDIRECT)? bno%NINDIRECT : 0;
        {
          sref<buf> bp2 = buf::get(ip->dev, a1[i]);
          auto locked2 = bp2->write();
          u32* a2 = (u32*)locked2->data;
          for (int j = start; j < NINDIRECT; j++) {
            if (!a2[j])
              continue;

            bfree(ip->dev, a2[j], trans, true);
            a2[j] = 0;
          }
          if (trans && start != 0)
            bp2->add_to_transaction(trans);
        }

        if (start == 0) {
          bfree(ip->dev, a1[i], trans, true);
          a1[i] = 0;
        }
      }
      if (trans && bno != 0)
        bp1->add_to_transaction(trans);
    }

    if (bno == 0) {
      bfree(ip->dev, ip->addrs[NDIRECT+1], trans, true);
      ip->addrs[NDIRECT+1] = 0;
    }
  }

  ip->size = offset;
}


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
