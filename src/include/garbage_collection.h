/**
 * @file
 * @brief about garbage collection
 */

#pragma once

#include <array>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "epoch.h"
#include "record.h"

namespace kvs {

/**
 * @brief Delete std::vector<Record*> kGarbageRecords at
 * shirakami/src/gcollection.cc
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void delete_all_garbage_records();

/**
 * @brief Delete first of std::pair<std::string*, epoch_t>> kGarbageValues at
 * shirakami/src/gcollection.cc
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void delete_all_garbage_values();

/**
 * @brief Release all heap objects in this system.
 * @details Do three functions: delete_all_garbage_values(),
 * delete_all_garbage_records(), and remove_all_leaf_from_mtdb_and_release().
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void release_all_heap_objects();

/**
 * @brief Remove all leaf nodes from MTDB and release those heap objects.
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void remove_all_leaf_from_mtdb_and_release();

extern std::array<std::vector<Record*>, KVS_NUMBER_OF_LOGICAL_CORES>
    kGarbageRecords;
extern std::array<std::mutex, KVS_NUMBER_OF_LOGICAL_CORES> kMutexGarbageRecords;
extern std::array<std::vector<std::pair<std::string*, epoch::epoch_t>>,
                  KVS_NUMBER_OF_LOGICAL_CORES>
    kGarbageValues;
extern std::array<std::mutex, KVS_NUMBER_OF_LOGICAL_CORES> kMutexGarbageValues;

}  // namespace kvs
