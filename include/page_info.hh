#pragma once

#include "ref.hh"
#include "refcache.hh"
#include "snzi.hh"
#include "gc.hh"
#include "types.h"
#include "oplog.hh"

#include <cstddef>
#include <vector>

#define MAX_PENDING_OPS 100

using namespace oplog;

// Per-allocation debug information
struct alloc_debug_info
{
#if KERNEL_HEAP_PROFILE
  // Instruction pointer of allocation
  const void *kalloc_rip_, *kmalloc_rip_;
  void set_kalloc_rip(const void *kalloc_rip) { kalloc_rip_ = kalloc_rip; }
  const void *kalloc_rip() const { return kalloc_rip_; }
  void set_kmalloc_rip(const void *kmalloc_rip) { kmalloc_rip_ = kmalloc_rip; }
  const void *kmalloc_rip() const { return kmalloc_rip_; }
#else
  void set_kalloc_rip(const void *kalloc_rip) { }
  const void *kalloc_rip() const { return nullptr; }
  void set_kmalloc_rip(const void *kmalloc_rip) { }
  const void *kmalloc_rip() const { return nullptr; }
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
    kfree(va());
    rmap_pte->delete_rmap();
  }

public:
  typedef std::pair<u64, uptr> rmap_entry;
  class rmap: public tsc_logged_object {
    public:
      NEW_DELETE_OPS(rmap);
      rmap() { num_pending_ops_ = 0; }
      struct add_op {
        add_op(rmap *p, rmap_entry map): parent(p), mapping(map) { }
        void operator()() {
          //cprintf("PA:(%016lX) Adding mapping for proc %ld: va %016lX\n", 
          //            (u64)parent, mapping.first, mapping.second);
          std::vector<rmap_entry>::iterator it;
          it = std::find(parent->rmap_vec.begin(), parent->rmap_vec.end(), mapping);
          if(it == parent->rmap_vec.end())
            parent->rmap_vec.push_back(std::move(mapping));
        }
        private:
        rmap *parent;
        rmap_entry mapping;
      };

      struct rem_op {
        rem_op(rmap *p, rmap_entry map): parent(p), mapping(map) { }
        void operator()() {
          if (parent->rmap_vec.size() == 0) return;
          //cprintf("PA:(%016lX) Removing mapping for proc %ld: va %016lX\n", 
          //            (u64)parent, mapping.first, mapping.second);
          std::vector<rmap_entry>::iterator it;
          it = std::find(parent->rmap_vec.begin(), parent->rmap_vec.end(), mapping);
          if(it != parent->rmap_vec.end()) {
            it->first = parent->rmap_vec.back().first;
            it->second = parent->rmap_vec.back().second;
            parent->rmap_vec.pop_back();
          }
        }
        private:
        rmap *parent;
        rmap_entry mapping;
      };

      void add_mapping(rmap_entry map) {
        add_op addop(this, map);
        get_logger()->push<add_op>(std::forward<add_op>(addop));
        num_pending_ops_++;
        if (num_pending_ops_ > MAX_PENDING_OPS)
          sync();
      }

      void remove_mapping(rmap_entry map) {
        rem_op remop(this, map);
        get_logger()->push<rem_op>(std::forward<rem_op>(remop));
        num_pending_ops_++;
        if (num_pending_ops_ > MAX_PENDING_OPS)
          sync();
      }

      void delete_rmap() {
        rmap_vec.clear();
      }

      void sync() {
        synchronize();
        num_pending_ops_ = 0;
      }

    protected:
      std::vector<rmap_entry> rmap_vec;

    private:
      int num_pending_ops_;
  };
  
  page_info() {
    rmap_pte = new rmap();
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

  void add_pte(rmap_entry map) {
    rmap_pte->add_mapping(map);
  }

  void remove_pte(rmap_entry map) {
    rmap_pte->remove_mapping(map);
  }

private:
  rmap *rmap_pte;

};

