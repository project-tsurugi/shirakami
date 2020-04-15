/**
 * @file
 * @brief about garbage collection
 */

#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "epoch.hh"
#include "record.hh"

namespace kvs {

/**
 * @brief Delete std::vector<Record*> kGarbageRecords at kvs_charkey/src/gcollection.cc
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void delete_all_garbage_records();

/**
 * @brief Delete first of std::pair<std::string*, Epoch>> kGarbageValues at kvs_charkey/src/gcollection.cc
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void delete_all_garbage_values();

extern std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::vector<std::pair<std::string*, Epoch>> kGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::mutex kMutexGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];

} // namespace kvs
