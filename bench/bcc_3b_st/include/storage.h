/**
 * @file init.h
 */

#pragma once

#include <vector>

#include "shirakami/storage_options.h"

using storages_type = std::vector<shirakami::Storage>;

/**
 * @brief for online tx workers.
 * @details The thread id corresponds to the index position.
 */
inline storages_type ol_storages{}; // NOLINT

/**
 * @brief for batch tx workers.
 * @details The thread id corresponds to the index position.
 */
inline storages_type bt_storages{}; // NOLINT

/**
 * global variable's getter
 */
static inline storages_type& get_ol_storages() { return ol_storages; }

static inline storages_type& get_bt_storages() { return bt_storages; }

extern void init_db();
