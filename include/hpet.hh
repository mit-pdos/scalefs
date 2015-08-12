#pragma once

#include <stdint.h>
#include "types.h"
#include "cpuid.hh"
#include "amd64.h"

class hpet
{
public:
  constexpr hpet() : base_(nullptr), period_fsec_(0) { }

  bool register_base(uintptr_t base);

  uint64_t read_nsec() const;

private:
  struct reg;
  struct reg *base_;

  uint64_t period_fsec_;
};

extern class hpet *the_hpet;

static inline u64 get_tsc()
{
  if (cpuid::features().rdtscp)
    return rdtscp();

  // If rdtscp is not supported, use the (more expensive) combination
  // cpuid + rdtsc as an alternative.
  return rdtsc_serialized();
}
