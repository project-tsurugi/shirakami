
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
 * @brief read record by using dest given by caller and store read info to res given by caller.
 * @param [out] res it is stored read info.
 * @param [in] dest read record pointed by this dest.
 * @pre the dest wasn't read in the same transaction.
 * @return void
 */
static void read_record(Record& res, Record* dest);
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
