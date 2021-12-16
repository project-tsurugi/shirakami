/**
 * @file init.h
 */

#pragma once

#include "shirakami/scheme.h"

using storages_type = std::vector<shirakami::Storage>;

/**
 * @brief for batch tx workers.
 * @details The thread id corresponds to the index position.
 */
inline storages_type bt_storages{}; // NOLINT

/**
 * global variable's getter
 */

static inline storages_type& get_bt_storages() { return bt_storages; }

extern void init_db();