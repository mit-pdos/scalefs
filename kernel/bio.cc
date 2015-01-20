#include "types.h"
#include "kernel.hh"
#include "buf.hh"
#include "weakcache.hh"
#include "mfs.hh"
#include "scalefs.hh"


static weakcache<buf::key_t, buf> bufcache(BSIZE << 10);
            //I don't think this was changed when BSIZE was changed from 512 to 4096

sref<buf>
buf::get(u32 dev, u64 block)
{
  buf::key_t k = { dev, block };
  for (;;) {
    sref<buf> b = bufcache.lookup(k);
    if (b.get() != nullptr) {
      // Wait for buffer to load, by getting a read seqlock,
      // which waits for the write seqlock bit to be cleared.
      b->seq_.read_begin();
      return b;
    }

    sref<buf> nb = sref<buf>::transfer(new buf(dev, block));
    auto locked = nb->write(); // marks the block as dirty automatically
    if (bufcache.insert(k, nb.get())) {
      nb->cache_pin(true); // keep it in the cache
      ideread(dev, locked->data, BSIZE, block*BSIZE);
      nb->mark_clean(); // we just loaded the contents from the disk!
      return nb;
    }
  }
}

// Evict a (clean) block from the buffer-cache
void
buf::put(u32 dev, u64 block)
{
  buf::key_t k = { dev, block };

  sref<buf> bp = bufcache.lookup(k);
  if (bp.get() != nullptr) {
    auto locked = bp->write_clean();
    if (!bp->dirty()) {
      bp->cache_pin(false); // drop it from the cache
    }
  }
}

void
buf::writeback()
{
  lock_guard<sleeplock> l(&writeback_lock_);

  // We need to mark the buf as clean at this point (even though we haven't
  // written it out yet) because a concurrent writer can update the block's
  // contents (note that we haven't acquired the write_lock_). Such an update
  // will mark the buf as dirty; so if we postpone marking the buf as clean
  // here, we will risk losing the dirty-bit, since we might overwrite it.
  //
  // One might wonder if there is a risk of freeing up the buf prematurely
  // during eviction, since the buf is marked clean even before it has been
  // written out. Luckily this won't happen because the buf is still alive
  // (in scope) as long as we are executing this function.
  // However, asynchronous disk-writes might get us into trouble!
  mark_clean();
  auto copy = read();

  // This write has to be synchronous, so that we don't free up the buf
  // while the copy operation is still in progress. Supporting asynchronous
  // disk-writes would need a more careful design, to manage the buf's
  // lifetime properly!
  idewrite(dev_, copy->data, BSIZE, block_*BSIZE);
}

// Must be invoked with the buf's write_lock_ held.
void
buf::add_to_transaction(transaction *trans)
{
  // It doesn't matter whether we mark it clean before or after we add it to
  // the transaction, as long as we mark it clean while still holding the
  // write_lock_. But we mark it clean early, just to be consistent with
  // ->writeback().
  mark_clean();

  // We can't issue a read() to read the contents of the buf because we are
  // already holding the seq-lock for write (hence we'll end up in a self-
  // deadlock if we do so). So read directly from data_ instead.
  trans->add_unique_block(block_, data_->data);
}

void
buf::onzero()
{
  bufcache.cleanup(weakref_);
  delete this;
}
