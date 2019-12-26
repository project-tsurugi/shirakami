#pragma once

/**
 * @file
 * @brief core work about kvs.
 */

namespace kvs {

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
