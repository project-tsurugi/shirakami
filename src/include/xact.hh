
/**
 * @file
 * @brief private transaction engine interface
 */

#pragma once

#include "include/header.hh"
#include "include/scheme.hh"

#include "kvs/scheme.h"

using namespace std;

#define LOG_FILE "/tmp/LogFile"
#define KVS_EPOCH_TIME 40 // ms

namespace kvs {

extern std::vector<LogShell> kLogList;
extern std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;
/* kGarbageRecords is a list of garbage records.
 * Theoretically, each worker thread has own list.
 * But in this kvs, the position of core at which worker is may change.
 * This is problem. It prepare enough list for experiments as pending solution.*/
extern std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
extern std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
extern MasstreeWrapper<Record> MTDB;

/**
 * @brief epoch thread
 * @pre this function is called by invoke_core_thread function.
 */
void epocher();

/**
 * @brief find record from masstree by using args informations.
 * @return the found record pointer.
 */
static Record* find_record_from_masstree(char const *key, std::size_t len_key);

/**
 * @brief insert record to masstree by using args informations.
 * @pre the record which has the same key as the key of args have never been inserted.
 * @param record It inserts this pointer to masstree database.
 */
static void insert_record_to_masstree(char const *key, std::size_t len_key, Record* record);

/**
 * @brief read record by using dest given by caller and store read info to res given by caller.
 * @pre the dest wasn't already read by itself.
 * @param [out] res it is stored read info.
 * @param [in] dest read record pointed by this dest.
 * @return Status::OK, it was ended correctly.
 * @return Status::ERR_ILLEGAL_STATE, other thread is inserting this record concurrently, 
 * but it isn't committed yet.
 */
static Status read_record(Record& res, Record* dest);

static void gc_records();

}  // namespace kvs
