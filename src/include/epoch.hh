#pragma once

// kvs_charkey/src/
#include "include/header.hh"

namespace kvs {

extern uint64_t kGlobalEpoch;
extern uint64_t kReclamationEpoch;

static inline uint64_t
load_acquire_ge()
{
  return __atomic_load_n(&(kGlobalEpoch), __ATOMIC_ACQUIRE);
}

static inline void
atomic_add_global_epoch()
{
  uint64_t expected = load_acquire_ge();
  for (;;) {
    uint64_t desired = expected + 1;
    if (__atomic_compare_exchange_n(&(kGlobalEpoch), &(expected), desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
      break;
    }
  }
}

} // namespace kvs.
