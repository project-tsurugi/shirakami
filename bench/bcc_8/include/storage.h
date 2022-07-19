/**
 * @file init.h
 */

#pragma once

#include "shirakami/storage_options.h"

/**
 * @brief for tx workers.
 * @details The thread id corresponds to the index position.
 */
inline shirakami::Storage st_{}; // NOLINT

/**
 * global variable's getter
 */
static inline shirakami::Storage get_st() { return st_; }

static inline void set_st(shirakami::Storage st) { st_ = st; }

extern void init_db();