//
// Allocate objects smaller than a page.
//

#include "types.h"
#include "mmu.h"
#include "kernel.hh"
#include "spinlock.hh"
#include "kalloc.hh"
#include "mtrace.h"
#include "cpu.hh"
#include "kstream.hh"
#include "log2.hh"
#include "rnd.hh"
#include "amd64.h"
#include "page_info.hh"
#include "heapprof.hh"
#include "numa.hh"

#include <type_traits>

// allocate in power-of-two sizes up to 2^KMMAX (PGSIZE)
#define KMMAX PGSHIFT

struct header {
  struct header *next;
};

struct bucket {
  spinlock lock;
  header* hdr;
  u64 count;
};

struct freelist {
  struct bucket buckets[KMMAX+1];
  char name[MAXNAME];
};

DEFINE_PERCPU(struct freelist, freelists);

void
kminit(void)
{
  for (int c = 0; c < ncpu; c++) {
    freelists[c].name[0] = (char) c + '0';
    safestrcpy(freelists[c].name+1, "freelist", MAXNAME-1);
    for (int b = 0; b < KMMAX+1; b++) {
      scoped_acquire guard(&freelists[c].buckets[b].lock);
      freelists[c].buckets[b].hdr = nullptr;
      freelists[c].buckets[b].count = 0;
    }
  }
}

// get more space for freelists[c].buckets[b]
static int
morecore(int c, int b)
{
  char *p = kalloc("kmalloc", PGSIZE, c);
  if(p == 0)
    return -1;

  if (ALLOC_MEMSET)
    memset(p, 3, PGSIZE);

#if RANDOMIZE_KMALLOC
#if CODEX
  u8 r = rnd() % 11;
#else
  u8 r = rdtsc() % 11;
#endif
#else
  u8 r = 0;
#endif

  int sz = 1 << b;
  assert(sz >= sizeof(header));
  scoped_acquire guard(&freelists[c].buckets[b].lock);
  for(char *q = p + CACHELINE * r; q + sz <= p + PGSIZE; q += sz){
    struct header *h = (struct header *) q;
    h->next = freelists[c].buckets[b].hdr;
    freelists[c].buckets[b].hdr = h;
    freelists[c].buckets[b].count++;
  }

  return 0;
}

static int
bucket(u64 nbytes)
{
  int b = ceil_log2(nbytes);
  if (b < 6)
    b = 6;
  assert((1<<b) >= nbytes);
  assert(b <= KMMAX);
  return b;
}

static void *
kmalloc_small(size_t b, const char *name, int cpu)
{
  struct header *h;
  int c = cpu >= 0 ? cpu : mycpu()->id;

  for (;;) {
    scoped_acquire guard(&freelists[c].buckets[b].lock);
    h = freelists[c].buckets[b].hdr;
    if (h) {
      freelists[c].buckets[b].hdr = h->next;
      freelists[c].buckets[b].count--;
      break;
    }

    guard.release();
    if (morecore(c, b) < 0) {
      cprintf("kmalloc(%d) failed\n", 1 << b);
      return 0;
    }
  }

  if (ALLOC_MEMSET) {
    char* chk = (char*)h + sizeof(struct header);
    for (int i = 0; i < (1<<b)-sizeof(struct header); i++)
      if (chk[i] != 3) {
        console.print(shexdump(chk, 1<<b));
        panic("kmalloc: free memory was overwritten %p+%x", chk, i);
      }
    memset(h, 4, (1<<b));
  }

  return h;
}

void *
kmalloc(u64 nbytes, const char *name, int cpu)
{
  void *h;
  uint64_t mbytes = alloc_debug_info::expand_size(nbytes);

  if (mbytes > PGSIZE / 2) {
    // Full page allocation
    h = kalloc(name, round_up_to_pow2(mbytes), cpu);
  } else {
    // Sub-page allocation
    int b = bucket(mbytes);
    h = kmalloc_small(b, name, cpu);
  }
  if (!h)
    return nullptr;

  // Update debug_info
  alloc_debug_info *adi = alloc_debug_info::of(h, nbytes);
  if (KERNEL_HEAP_PROFILE) {
    auto alloc_rip = __builtin_return_address(0);
    if (heap_profile_update(HEAP_PROFILE_KMALLOC, alloc_rip, nbytes))
      adi->set_kmalloc_rip(alloc_rip);
    else
      adi->set_kmalloc_rip(nullptr);
  }

  mtlabel(mtrace_label_heap, (void*) h, nbytes, name, strlen(name));

  return h;
}

void
kmfree(void *ap, u64 nbytes)
{
  struct header *h = (struct header *) ap;
  mtunlabel(mtrace_label_heap, ap);

  // Update debug_info
  alloc_debug_info *adi = alloc_debug_info::of(ap, nbytes);
  if (KERNEL_HEAP_PROFILE) {
    auto alloc_rip = adi->kmalloc_rip();
    if (alloc_rip)
      heap_profile_update(HEAP_PROFILE_KMALLOC, alloc_rip, -nbytes);
  }

  if (nbytes > PGSIZE / 2) {
    // Free full page allocation
    kfree(ap, round_up_to_pow2(nbytes));
  } else {
    // Free sub-page allocation
    int b = bucket(nbytes);

    if (ALLOC_MEMSET)
      memset(ap, 3, (1<<b));

    int c = mycpu()->id;
    scoped_acquire guard(&freelists[c].buckets[b].lock);
    h->next = freelists[c].buckets[b].hdr;
    freelists[c].buckets[b].hdr = h;
    freelists[c].buckets[b].count++;
  }
}

int
kmalign(void **p, int align, u64 size, const char *name)
{
  void *mem = kmalloc(size + (align-1) + sizeof(void*), name);
  char *amem = ((char*)mem) + sizeof(void*);
  amem += align - ((uptr)amem & (align - 1));
  ((void**)amem)[-1] = mem;
  *p = amem;
  return 0;
}

void kmalignfree(void *mem, int align, u64 size)
{
  u64 msz = size + (align-1) + sizeof(void*);
  kmfree(((void**)mem)[-1], msz);
}

// Expand an allocation size to include its alloc_debug_info
size_t
alloc_debug_info::expand_size(size_t size)
{
  if (std::is_empty<alloc_debug_info>::value)
    return size;

  static_assert(round_up_to_pow2_const(alignof(alloc_debug_info)) ==
                alignof(alloc_debug_info),
                "alignof(alloc_debug_info) isn't a power of two");
  size_t aligned = (size + (alignof(alloc_debug_info) - 1)) &
    ~(alignof(alloc_debug_info) - 1);
  size_t want = aligned + sizeof(alloc_debug_info);
  if (want > PGSIZE / 2) {
    // Store alloc_debug_info in page_info.  Round the size up
    // enough to make sure it allocates a whole page (we can't just
    // return size, because that may be <= PGSIZE / 2)
    if (size <= PGSIZE / 2)
      // We can't just return size because that would cause a
      // sub-page allocation, so make it just big enough to force a
      // full page allocation.
      return PGSIZE / 2;
    return size;
  }
  // Sub-page allocations store the alloc_debug_info at the end
  return want;
}

// Given an allocated pointer and the allocation's original size (not
// its expanded size), return the alloc_debug_info for the allocation.
alloc_debug_info *
alloc_debug_info::of(void *p, size_t size)
{
  if (std::is_empty<alloc_debug_info>::value)
    return nullptr;

  size_t aligned = (size + (alignof(alloc_debug_info) - 1)) &
    ~(alignof(alloc_debug_info) - 1);
  size_t want = aligned + sizeof(alloc_debug_info);

  if (want > PGSIZE / 2)
    return page_info::of(p);
  return (alloc_debug_info*)((char*)p + aligned);
}

void
kmbalance(void)
{
  for (int b = 0; b < KMMAX+1; b++) {
    for (auto &node : numa_nodes)
      for (auto cpuid : node.cpuids)
        freelists[cpuid].buckets[b].lock.acquire();

    u64 total = 0;
    u64 ncpu = 0;
    for (auto &node : numa_nodes) {
      for (auto cpuid : node.cpuids) {
        total += freelists[cpuid].buckets[b].count;
        ncpu++;
      }
    }

    header* extras = nullptr;
    for (auto &node : numa_nodes) {
      for (auto cpuid : node.cpuids) {
        while (freelists[cpuid].buckets[b].count > total/ncpu) {
          header* h = freelists[cpuid].buckets[b].hdr;
          assert(h);
          freelists[cpuid].buckets[b].hdr = h->next;
          freelists[cpuid].buckets[b].count--;
          h->next = extras;
          extras = h;
        }
      }
    }

    for (auto &node : numa_nodes) {
      for (auto cpuid : node.cpuids) {
        while (extras && freelists[cpuid].buckets[b].count <= total/ncpu + 1) {
          header* h = extras;
          extras = h->next;
          h->next = freelists[cpuid].buckets[b].hdr;
          freelists[cpuid].buckets[b].hdr = h;
          freelists[cpuid].buckets[b].count++;
        }
      }
    }

    for (auto &node : numa_nodes)
      for (auto cpuid : node.cpuids)
        freelists[cpuid].buckets[b].lock.release();

    assert(!extras);
  }
}
