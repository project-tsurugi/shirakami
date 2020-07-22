/**
 * @file
 * @brief core work about shirakami.
 */

#pragma once

#include "xact.h"

namespace shirakami {

/**
 * @brief init work about kThreadTable
 */
static void init_kThreadTable();

/**
 * @brief fin work about kThreadTable
 */
static void fin_kThreadTable();

/**
 * @brief invoke core threads about shirakami.
 */
static void invoke_core_thread();

}  // namespace shirakami
