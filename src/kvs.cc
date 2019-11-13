
#include "include/debug.h"
#include "include/kvs.h"
#include "include/port.h"

#include <cstdint>
#include "kvs/interface.h"

namespace kvs {

pthread_t EpochThread;
pthread_t LogThread;
uint64_t kGlobalEpoch;
pthread_mutex_t kMutexLogList;
pthread_mutex_t kMutexThreadTable;

void
invoke_logger(void)
{
  int ret = pthread_create(&LogThread, nullptr, logger, nullptr);      
  if (ret < 0) ERR;
}

void
invoke_epocher(void)
{
  int ret = pthread_create(&EpochThread, nullptr, epocher, nullptr);      
  if (ret < 0) ERR;
}

static void
init_mutex(void)
{
  //pthread_mutex_init(&kMutexLogList, nullptr);
  pthread_mutex_init(&kMutexThreadTable, nullptr);
}

static void
invoke_core_thread(void)
{
  invoke_epocher();
  //invoke_logger();
}

extern void
init()
{
  init_mutex();
  invoke_core_thread();
}

}  // namespace kvs
