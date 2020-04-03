/**
 * @file epoch.hh
 * @brief header about epoch
 */

#pragma once

#include <atomic>
#include <thread>

// kvs_charkey/src/
#include "include/header.hh"

namespace kvs {

extern uint64_t kGlobalEpoch;
extern uint64_t kReclamationEpoch;

// about epoch thread
extern std::thread kEpochThread;
extern std::atomic<bool> kEpochThreadEnd;

extern void atomic_add_global_epoch();

extern bool check_epoch_loaded();

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

extern uint64_t load_acquire_ge();

} // namespace kvs.
