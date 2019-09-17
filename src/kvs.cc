#include "kernel.h"
#include "debug.h"
#include "port.h"
#include <cstdint>
#include "kvs.h"
#include "interface.h"

namespace kvs {

pthread_t EpochThread;
pthread_t LogThread;
uint64_t GlobalEpoch;
pthread_mutex_t MutexLogList;
pthread_mutex_t MutexDB;
pthread_mutex_t MutexToken;
pthread_mutex_t MutexThreadTable;

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
  //pthread_mutex_init(&MutexLogList, nullptr);
  pthread_mutex_init(&MutexThreadTable, nullptr);
  pthread_mutex_init(&MutexDB, nullptr);
	pthread_mutex_init(&MutexToken, nullptr);
}

static void
invoke_core_thread(void)
{
  invoke_epocher();
  //invoke_logger();
}

extern void
kvs_init(void)
{
  init_mutex();
  invoke_core_thread();
}

}  // namespace kvs
