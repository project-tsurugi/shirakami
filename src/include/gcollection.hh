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

extern std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::vector<std::pair<std::string*, Epoch>> kGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];

extern void gc_records();

} // namespace kvs
