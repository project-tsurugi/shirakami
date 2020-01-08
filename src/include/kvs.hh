#pragma once

/**
 * @file
 * @brief core work about kvs.
 */

namespace kvs {

extern std::thread kEpochThread;
extern std::atomic<bool> kEpochThreadEnd;
extern uint64_t kGlobalEpoch;
extern uint64_t kReclamationEpoch;

extern void epocher();

extern void init();
extern void fin();

/**
 * @brief init work about kThreadTable
 */
static void init_kThreadTable();

/**
 * @brief invoke core threads about kvs.
 */
static void invoke_core_thread();

/**
 * @brief invoke epocher thread.
 * @post invoke fin() to join this thread.
 */
static void invoke_epocher();

}  // namespace kvs
