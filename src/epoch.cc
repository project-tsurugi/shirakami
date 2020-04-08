/**
 * @file epoch.cc
 * @brief implement about epoch
 */

#include <xmmintrin.h>

#include "atomic_wrapper.hh"
#include "clock.hh"
#include "compiler.hh"
#include "cpu.hh"
#include "epoch.hh"
#include "xact.hh"

namespace kvs {

std::thread kEpochThread;
std::atomic<bool> kEpochThreadEnd;
Epoch kGlobalEpoch(1);
Epoch kReclamationEpoch(0);

void
atomic_add_global_epoch()
{
  uint64_t expected = load_acquire_ge();
  for (;;) {
    uint64_t desired = expected + 1;
    if (__atomic_compare_exchange_n(&(kGlobalEpoch), &(expected), desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
      break;
    }
  }
}

bool
check_epoch_loaded()
{
  uint64_t curEpoch = load_acquire_ge();

  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr){
    if (itr->get_visible() == true 
        //&& loadAcquire(itr->epoch_) != curEpoch) {
        && itr->get_epoch() != curEpoch) {
      return false;
    }
  }

  return true;
}

void
epocher() 
{
  // Increment global epoch in each 40ms.
  // To increment it, 
  // all the worker-threads need to read the latest one.
 
#ifdef KVS_Linux
  setThreadAffinity(static_cast<int>(CorePosition::EPOCHER));
#endif

  while (likely(kEpochThreadEnd.load(std::memory_order_acquire) == false)) {
    sleepMs(KVS_EPOCH_TIME);

    // check_epoch_loaded() checks whether the 
    // latest global epoch is read by all the threads
    while (!check_epoch_loaded()) { 
      _mm_pause(); 
    }

    atomic_add_global_epoch();
    storeRelease(kReclamationEpoch, loadAcquire(kGlobalEpoch) - 2);
  }
}

void
invoke_epocher()
{
  kEpochThreadEnd.store(false, std::memory_order_release);
  kEpochThread = std::thread(epocher);
}

uint64_t
load_acquire_ge()
{
  return __atomic_load_n(&(kGlobalEpoch), __ATOMIC_ACQUIRE);
}

} // namespace kvs
