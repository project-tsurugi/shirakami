
/**
 * @file
 * @brief private transaction engine interface
 */

#pragma once

#include "masstree_wrapper.hh"
#include "scheme.hh"

#include "kvs/scheme.h"

using namespace std;

#define KVS_EPOCH_TIME 40 // ms

namespace kvs {

extern std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;
extern MasstreeWrapper<Record> MTDB;

/**
 * @brief find record from masstree by using args informations.
 * @return the found record pointer.
 */
extern Record* find_record_from_masstree(char const *key, std::size_t len_key);

/**
 * @brief insert record to masstree by using args informations.
 * @pre the record which has the same key as the key of args have never been inserted.
 * @param key
 * @param len_key
 * @param record It inserts this pointer to masstree database.
 * @return WARN_ALREADY_EXISTS The records whose key is the same as @a key exists in masstree,
 * so this function returned immediately.
 * @return Status::OK It inserted record.
 */
extern Status insert_record_to_masstree(char const *key, std::size_t len_key, Record* record);

/**
 * @brief read record by using dest given by caller and store read info to res given by caller.
 * @pre the dest wasn't already read by itself.
 * @param [out] res it is stored read info.
 * @param [in] dest read record pointed by this dest.
 * @return WARN_CONCURRENT_DELETE No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::OK, it was ended correctly.
 * but it isn't committed yet.
 */
extern Status read_record(Record& res, const Record* const dest);

/**
 * @brief Transaction begins.
 * @details Get an epoch accessible to this transaction.
 * @return void
 */
extern void tbegin(Token token);

}  // namespace kvs
