/**
 * @file epoch.h
 * @brief header about epoch
 */

#pragma once

#include <atomic>
#include <thread>

#include "atomic_wrapper.h"

namespace shirakami::cc_silo_variant::epoch {

/**
 * @brief epoch_t
 * @details
 * tid_word is composed of union ...
 * 1bits : lock
 * 1bits : latest
 * 1bits : absent
 * 29bits : tid
 * 32 bits : epoch.
 * So epoch_t should be uint32_t.
 */
using epoch_t = std::uint32_t;

[[maybe_unused]] inline std::atomic<epoch_t> kGlobalEpoch;  // NOLINT
inline std::atomic<epoch_t> kReclamationEpoch;              // NOLINT

// about epoch thread
[[maybe_unused]] inline std::thread kEpochThread;  // NOLINT
inline std::atomic<bool> kEpochThreadEnd;          // NOLINT

[[maybe_unused]] extern bool check_epoch_loaded();  // NOLINT

/**
 * @brief epoch thread
 * @pre this function is called by invoke_core_thread function.
 */
[[maybe_unused]] extern void epocher();

[[maybe_unused]] static epoch::epoch_t get_reclamation_epoch() {  // NOLINT
    return kReclamationEpoch.load(std::memory_order_acquire);
}

/**
 * @brief invoke epocher thread.
 * @post invoke fin() to join this thread.
 */
[[maybe_unused]] static void invoke_epocher() {
    kEpochThreadEnd.store(false, std::memory_order_release);
    kEpochThread = std::thread(epocher);
}

[[maybe_unused]] static void join_epoch_thread() { kEpochThread.join(); }

[[maybe_unused]] static void set_epoch_thread_end(const bool tf) {
    kEpochThreadEnd.store(tf, std::memory_order_release);
}

}  // namespace shirakami::cc_silo_variant::epoch