/**
 * @file epoch.cpp
 * @brief implement about epoch
 */

#include "cc/silo_variant/include/epoch.h"

#include "clock.h"
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif
#include "cc/silo_variant/include/thread_info_table.h"
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami::silo_variant {

void epoch::atomic_add_global_epoch() {
  std::uint32_t expected = load_acquire_global_epoch();
  for (;;) {
    std::uint32_t desired = expected + 1;
    if (__atomic_compare_exchange_n(&(kGlobalEpoch), &(expected), desired,
                                    false, __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
      break;
    }
  }
}

bool epoch::check_epoch_loaded() {  // NOLINT
  uint64_t curEpoch = load_acquire_global_epoch();

  for (auto&& itr : thread_info_table::get_thread_info_table()) {
    if (itr.get_visible() && itr.get_epoch() != curEpoch) {
      return false;
    }
  }

  return true;
}

void epoch::epocher() {
  /**
   * Increment global epoch in each 40ms.
   * To increment it,
   * all the worker-threads need to read the latest one.
   */
  while (likely(!kEpochThreadEnd.load(std::memory_order_acquire))) {
    sleepMs(KVS_EPOCH_TIME);

    /**
     * check_epoch_loaded() checks whether the
     * latest global epoch is read by all the threads
     */
    while (!check_epoch_loaded()) {
      if (kEpochThreadEnd.load(std::memory_order_acquire)) return;
      _mm_pause();
    }

    atomic_add_global_epoch();
    storeRelease(kReclamationEpoch, loadAcquire(kGlobalEpoch) - 2);
  }
}

void epoch::invoke_epocher() {
  kEpochThreadEnd.store(false, std::memory_order_release);
  kEpochThread = std::thread(epocher);
}

std::uint32_t epoch::load_acquire_global_epoch() {  // NOLINT
  return loadAcquire(epoch::kGlobalEpoch);
}

}  // namespace shirakami::silo_variant