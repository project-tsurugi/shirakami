#pragma once

using namespace std;
#define EPOCH_TIME (40) // ms
#define CLOCK_PER_US (1000)
#define LOG_FILE "/tmp/LogFile"

namespace kvs {

extern uint64_t kGlobalEpoch;
extern pthread_mutex_t kMutexThreadTable;
extern pthread_mutex_t kMutexLogList;
extern pthread_mutex_t kMutexDB;
extern pthread_mutex_t kMutexToken;

extern void print_MTDB(void); 
}  // namespace kvs
