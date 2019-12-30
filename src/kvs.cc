
#include "include/cache_line_size.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/header.hh"
#include "include/kvs.hh"
#include "include/port.h"
#include "include/scheme.hh"

#include <cstdint>
#include "kvs/interface.h"

using std::cout;
using std::endl;

namespace kvs {

std::thread kEpochThread;
std::atomic<bool> kEpochThreadEnd;
uint64_t kGlobalEpoch(1);
uint64_t kReclamationEpoch(0);
extern std::array<ThreadInfo, KVS_MAX_PARALLEL_THREADS> kThreadTable;

static void
invoke_epocher()
{
  kEpochThreadEnd.store(false, std::memory_order_release);
  kEpochThread = std::thread(epocher);
}

static void
invoke_core_thread()
{
  invoke_epocher();
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
  init_kThreadTable();
  invoke_core_thread();
}

extern void
fin()
{
  kEpochThreadEnd.store(true, std::memory_order_release);
  kEpochThread.join();
}

}  // namespace kvs
