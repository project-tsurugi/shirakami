/**
 * @file garbage_collection.h
 * @brief about garbage collection
 */

#pragma once

#include <array>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "cpu.h"
#include "epoch.h"
#include "record.h"

namespace shirakami::garbage_collection {

/**
 * @brief Release all heap objects in this system.
 * @details Do three functions: delete_all_garbage_values(),
 * delete_all_garbage_records(), and remove_all_leaf_from_mt_db_and_release().
 * @pre This function should be called at terminating db.
 * @return void
 */
[[maybe_unused]] extern void release_all_heap_objects();

/**
 * @brief Remove all leaf nodes from MTDB and release those heap objects.
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void remove_all_leaf_from_mt_db_and_release();

}  // namespace shirakami::garbage_collection
