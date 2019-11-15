#pragma once

#include "include/header.hh"

#include "kvs/scheme.h"

using namespace std;

#define LOG_FILE "/tmp/LogFile"
#define KVS_EPOCH_TIME 40 // ms

namespace kvs {

extern pthread_mutex_t kMutexLogList;
extern pthread_mutex_t kMutexThreadTable;

extern void print_MTDB(void); 

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
