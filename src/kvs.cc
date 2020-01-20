
#include "include/cache_line_size.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/header.hh"
#include "include/kvs.hh"
#include "include/port.h"
#include "include/scheme.hh"
#include "include/xact.hh"

#include <cstdint>
#include "kvs/interface.h"

using std::cout;
using std::endl;

namespace kvs {

std::thread kEpochThread;
std::atomic<bool> kEpochThreadEnd;
uint64_t kGlobalEpoch(1);
uint64_t kReclamationEpoch(0);

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
  uint64_t ctr(0);
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    itr->visible.store(false, std::memory_order_release);
#ifdef WAL
    itr->log_dir_.assign(MAC2STR(PROJECT_ROOT));
    itr->log_dir_.append("/log/log");
    itr->log_dir_.append(std::to_string(ctr));
    ++ctr;
    if (!itr->logfile_.open(itr->log_dir_, O_CREAT | O_TRUNC | O_WRONLY, 0644)) {
      ERR;
    }
    //itr->logfile_.ftruncate(10^9); // if it want to be high performance in experiments, this line is used.
#endif
  }
}

void
init()
{
  init_kThreadTable();
  invoke_core_thread();
}

void
fin()
{
  kEpochThreadEnd.store(true, std::memory_order_release);
  kEpochThread.join();
}

}  // namespace kvs
