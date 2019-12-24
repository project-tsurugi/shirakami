
#include "include/cache_line_size.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/header.hh"
#include "include/kvs.h"
#include "include/port.h"
#include "include/scheme.h"

#include <cstdint>
#include "kvs/interface.h"

using std::cout;
using std::endl;

namespace kvs {

pthread_mutex_t kMutexLogList;
pthread_mutex_t kMutexThreadTable;
pthread_t EpochThread;
pthread_t LogThread;
uint64_t kGlobalEpoch(1);
uint64_t kReclamationEpoch(0);
extern std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;

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

static void
init_kThreadTable()
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    itr->visible.store(false, std::memory_order_release);
  }
}

extern void
init()
{
  init_mutex();
  init_kThreadTable();
  invoke_core_thread();
}

}  // namespace kvs
