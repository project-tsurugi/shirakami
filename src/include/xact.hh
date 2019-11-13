#pragma once

#include "include/header.hh"

#include "kvs/scheme.h"

using namespace std;

#define EPOCH_TIME (40) // ms
#define CLOCK_PER_US (1000)
#define LOG_FILE "/tmp/LogFile"

namespace kvs {

extern uint64_t kGlobalEpoch;
extern pthread_mutex_t kMutexThreadTable;
extern pthread_mutex_t kMutexLogList;

extern void print_MTDB(void); 

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
