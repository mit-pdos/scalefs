#pragma once

#include "ref.hh"
#include "refcache.hh"
#include "snzi.hh"
#include "gc.hh"
#include "types.h"
#include "oplog.hh"

#include <cstddef>
#include <vector>

using namespace oplog;

// Per-allocation debug information
struct alloc_debug_info
{
#if KERNEL_HEAP_PROFILE
  // Instruction pointer of allocation
  const void *kalloc_rip_, *kmalloc_rip_, *newarr_rip_;
  void set_kalloc_rip(const void *kalloc_rip) { kalloc_rip_ = kalloc_rip; }
  const void *kalloc_rip() const { return kalloc_rip_; }
  void set_kmalloc_rip(const void *kmalloc_rip) { kmalloc_rip_ = kmalloc_rip; }
  const void *kmalloc_rip() const { return kmalloc_rip_; }
  void set_newarr_rip(const void *newarr_rip) { newarr_rip_ = newarr_rip; }
  const void *newarr_rip() const { return newarr_rip_; }
#else
  void set_kalloc_rip(const void *kalloc_rip) { }
  const void *kalloc_rip() const { return nullptr; }
  void set_kmalloc_rip(const void *kmalloc_rip) { }
  const void *kmalloc_rip() const { return nullptr; }
  void set_newarr_rip(const void *newarr_rip) { }
  const void *newarr_rip() const { return nullptr; }
#endif

  static size_t expand_size(size_t size);
  static alloc_debug_info *of(void *p, size_t size);
};

// The page_info_map maps from physical address to page_info array.
// The map is indexed by
//   ((phys + page_info_map_add) >> page_info_map_shift)
// Since the page_info arrays live at the beginning of each region,
// this can also be used to find the page_info array of a given
// page_info*.
struct page_info_map_entry
{
  // The physical address of the page represented by array[0].
  paddr phys_base;
  // The page_info array, indexed by (phys - phys_base) / PGSIZE.
  class page_info *array;
};
extern page_info_map_entry page_info_map[256];
extern size_t page_info_map_add, page_info_map_shift;

// One past the last used entry of page_info_map.
extern page_info_map_entry *page_info_map_end;

// Physical page metadata
//
// This inherits from alloc_debug_info to exploit empty base class
// optimization.
class page_info : public PAGE_REFCOUNT referenced, public alloc_debug_info
{
protected:
  void onzero()
  {
    this->~page_info();
    kfree(va());
  }

public:
  typedef std::pair<vmap*, uptr> rmap_entry;
  // rmap is a reverse map of the vmaps that contain a mapping for the page
  // corresponding to this page_info. Rmap is implemented using oplog and is
  // used when a file page is no longer valid and the vmaps mapping it need to
  // be informed of the change.
  #define MAX_OPS 1000000
  class rmap: public tsc_logged_object {
    public:
      rmap(bool use_sleeplock) : tsc_logged_object(use_sleeplock) {}

      NEW_DELETE_OPS(rmap);
      // Oplog operation that implements adding a <vmap*,va> pair to the rmap
      struct add_op {
        add_op(rmap *p, rmap_entry map): parent(p), mapping(map) {
          assert(p);
        }
        void operator()() {
          std::vector<rmap_entry>::iterator it;
          it = std::find(parent->rmap_vec.begin(), parent->rmap_vec.end(), mapping);
          if(it == parent->rmap_vec.end())
            parent->rmap_vec.push_back(std::move(mapping));
        }
        void print() {
          cprintf("Rmap::add_op rmap:%016lX vmap:%016lX va:%016lX\n", 
                      (u64)parent, (u64)mapping.first, mapping.second); 
        }
        private:
        rmap *parent;
        rmap_entry mapping;
      };

      // Oplog operation that implements removing a <vmap*,va> pair from the rmap
      struct rem_op {
        rem_op(rmap *p, rmap_entry map): parent(p), mapping(map) {
          assert(p);
        }
        void operator()() {
          if (parent->rmap_vec.size() == 0) return;
          std::vector<rmap_entry>::iterator it;
          it = std::find(parent->rmap_vec.begin(), parent->rmap_vec.end(), mapping);
          if(it != parent->rmap_vec.end()) {
            it->first = parent->rmap_vec.back().first;
            it->second = parent->rmap_vec.back().second;
            parent->rmap_vec.pop_back();
          }
        }
        void print() {
          cprintf("Rmap::rem_op rmap:%016lX vmap:%016lX va:%016lX\n", 
                      (u64)parent, (u64)mapping.first, mapping.second);
        }
        private:
        rmap *parent;
        rmap_entry mapping;
      };

      void add_mapping(rmap_entry map) {
        get_logger(myid())->push<add_op>(add_op(this, map));
      }

      void remove_mapping(rmap_entry map) {
        get_logger(myid())->push<rem_op>(rem_op(this, map));
      }

      void sync(std::vector<rmap_entry> &vec) {
        auto guard = synchronize_with_spinlock();
        for (auto it = rmap_vec.begin(); it != rmap_vec.end(); it++)
          vec.emplace_back(*it);
        rmap_vec.clear();
      }

      void sync() {
        auto guard = synchronize_with_spinlock();
      }

    protected:
      std::vector<rmap_entry> rmap_vec;
  };

  page_info() {
    rmap_pte = new rmap(false); // use_sleeplock = false.
    for (int cpu = 0; cpu < NCPU; cpu++)
      outstanding_ops[cpu] = 0;
  }

  ~page_info() {
    delete rmap_pte;
  }

  // Only placement new is allowed, because page_info must only be
  // constructed in the page_info_array.
  static void* operator new(unsigned long nbytes, page_info *buf)
  {
    return buf;
  }

  // Return the page_info for the page containing the given physical
  // address.  This may return an uninitialized page_info.  It is up
  // to the caller to construct the page_info (using placement new)
  // for pages that require reference counting upon allocation.
  static page_info *
  of(paddr pa)
  {
    page_info_map_entry *entry =
      &page_info_map[(pa + page_info_map_add) >> page_info_map_shift];
    assert(entry < page_info_map_end);
    auto index = (pa - entry->phys_base) / PGSIZE;
    return &entry->array[index];
  }

  // Return the page_info for the page at direct-mapped virtual
  // address va.
  static page_info *
  of(void *va)
  {
    return of(v2p(va));
  }

  paddr pa() const
  {
    paddr info_pa = (paddr)this - KBASE;
    page_info_map_entry *entry =
      &page_info_map[(info_pa + page_info_map_add) >> page_info_map_shift];
    return entry->phys_base + (this - entry->array) * PGSIZE;
  }

  void *va() const
  {
    return p2v(pa());
  }

  // Add an entry to the rmap for this page
  void add_pte(rmap_entry map) {
    assert(rmap_pte);
    int cpu = myid();
    if (outstanding_ops[cpu] > MAX_OPS) {
      rmap_pte->sync();
      outstanding_ops[cpu] = 0;
    }
    rmap_pte->add_mapping(map);
    outstanding_ops[cpu]++;
  }

  // Remove an entry from the rmap for this page
  void remove_pte(rmap_entry map) {
    assert(rmap_pte);
    int cpu = myid();
    if (outstanding_ops[cpu] > MAX_OPS) {
      rmap_pte->sync();
      outstanding_ops[cpu] = 0;
    }
    rmap_pte->remove_mapping(map);
    outstanding_ops[cpu]++;
  }

  // Synchronizes the oplog-maintained rmap and returns the <vmap, vaddr> pairs
  // that have this page mapped.
  void get_rmap_vector(std::vector<rmap_entry> &vec) {
    assert(rmap_pte);
    int cpu = myid();
    rmap_pte->sync(vec);
    outstanding_ops[cpu] = 0;
  }

private:
  rmap *rmap_pte;
  percpu<u64> outstanding_ops;

} __attribute__((aligned(16)));

