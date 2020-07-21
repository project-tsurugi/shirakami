/**
 * @file epoch.h
 * @brief header about epoch
 */

#pragma once

#include <atomic>
#include <thread>

#include "atomic_wrapper.h"

namespace kvs {

class epoch {
public:
  /**
   * @brief epoch_t
   * @details
   * Tidword is composed of union ...
   * 1bits : lock
   * 1bits : latest
   * 1bits : absent
   * 29bits : tid
   * 32 bits : epoch.
   * So epoch_t should be uint32_t.
   */
  using epoch_t = std::uint32_t;

  static void atomic_add_global_epoch();

  static bool check_epoch_loaded();  // NOLINT

  /**
   * @brief epoch thread
   * @pre this function is called by invoke_core_thread function.
   */
  static void epocher();

  static epoch::epoch_t get_reclamation_epoch() {  // NOLINT
    return loadAcquire(kReclamationEpoch);
  }

  /**
   * @brief invoke epocher thread.
   * @post invoke fin() to join this thread.
   */
  static void invoke_epocher();

  static void join_epoch_thread() { kEpochThread.join(); }

  static std::uint32_t load_acquire_ge();  // NOLINT

  static void set_epoch_thread_end(bool tf) {
    kEpochThreadEnd.store(tf, std::memory_order_release);
  }

private:
  static inline epoch_t kGlobalEpoch;       // NOLINT
  static inline epoch_t kReclamationEpoch;  // NOLINT

  // about epoch thread
  static inline std::thread kEpochThread;           // NOLINT
  static inline std::atomic<bool> kEpochThreadEnd;  // NOLINT
};

}  // namespace kvs.
