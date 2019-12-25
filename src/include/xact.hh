
/**
 * @file
 * @brief private transaction engine interface
 */

#pragma once

#include "include/header.hh"
#include "include/scheme.h"

#include "kvs/scheme.h"

using namespace std;

#define LOG_FILE "/tmp/LogFile"
#define KVS_EPOCH_TIME 40 // ms

namespace kvs {

extern pthread_mutex_t kMutexLogList;
extern pthread_mutex_t kMutexThreadTable;
extern void print_MTDB(void); 

/**
 * @brief find record from masstree by using args informations.
 * @return the found record pointer.
 */
static Record* find_record_from_masstree(char const *key, std::size_t len_key);

/**
 * @brief insert record to masstree by using args informations.
 * @pre the record which has the same key as the key of args have never been inserted.
 * @param record it is used for notifing the inserted pointer.
 */
static void insert_record_to_masstree(char const *key, std::size_t len_key, char const *val, std::size_t len_val, Record** record);

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

/**
 * @brief unlock records in write set.
 * This function unlocked all records in write set absolutely.
 * So it has a pre-condition.
 * @pre It has locked all records in write set.
 * @return void
 */
static void unlock_write_set(std::vector<WriteSetObj>& write_set);

static void gc_records();

class TokenForExp {
  static std::atomic<Token> token_;

public:

  static Token GetToken() {
    return token_.fetch_add(1);
  }

  static void ResetToken() {
    token_.store(0, std::memory_order_release);
  }
};

#ifdef DECLARE_ENTITY_TOKEN_FOR_EXP
std::atomic<Token> TokenForExp::token_(0);
#endif
}  // namespace kvs
