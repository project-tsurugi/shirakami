/**
 * @file
 * @brief about garbage collection
 */

#pragma once

#include <mutex>
#include <vector>

#include "scheme.hh"

namespace kvs {

/* kGarbageRecords is a list of garbage records.
 * Theoretically, each worker thread has own list.
 * But in this kvs, the position of core at which worker is may change.
 * This is problem. It prepare enough list for experiments as pending solution.*/
extern std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];

extern void gc_records();

} // namespace kvs
