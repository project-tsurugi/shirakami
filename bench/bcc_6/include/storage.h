/**
 * @file init.h
 */

#pragma once

#include <vector>

#include "shirakami/storage_options.h"

using storages_type = std::vector<shirakami::Storage>;

/**
 * @brief for tx workers.
 * @details The thread id corresponds to the index position.
 */
inline storages_type storages{}; // NOLINT

/**
 * global variable's getter
 */
static inline storages_type& get_storages() { return storages; }

extern void init_db();
