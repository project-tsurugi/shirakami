/**
 * @file
 * @brief core work about kvs.
 */

#pragma once

#include "xact.h"

namespace kvs {

extern void init();

/**
 * @brief init work about kThreadTable
 */
static void init_kThreadTable();

/**
 * @brief fin work about kThreadTable
 */
static void fin_kThreadTable();

/**
 * @brief invoke core threads about kvs.
 */
static void invoke_core_thread();

}  // namespace kvs
