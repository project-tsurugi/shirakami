/**
 * @file epoch.hh
 * @brief header about epoch
 */

#pragma once

#include <atomic>
#include <thread>

namespace kvs {

/**
 * @brief Epoch class
 * @details
 * Tidword is composed of union ...
 * 1bits : lock
 * 1bits : latest
 * 1bits : absent
 * 29bits : tid
 * 32 bits : epoch.
 * So Epoch should be uint32_t.
 */
using Epoch = std::uint32_t;

extern void atomic_add_global_epoch();

extern bool check_epoch_loaded(); // NOLINT

/**
 * @brief epoch thread
 * @pre this function is called by invoke_core_thread function.
 */
extern void epocher();

/**
 * @brief invoke epocher thread.
 * @post invoke fin() to join this thread.
 */
extern void invoke_epocher();

extern std::uint32_t load_acquire_ge(); // NOLINT

extern Epoch kGlobalEpoch;
extern Epoch kReclamationEpoch;

// about epoch thread
extern std::thread kEpochThread;
extern std::atomic<bool> kEpochThreadEnd;

} // namespace kvs.
