/**
 * @file gcollection.cc
 * @brief about garbage collection.
 */

#include <utility>

#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "cpu.hh"
#include "epoch.hh"
#include "gcollection.hh"

namespace kvs {

alignas(CACHE_LINE_SIZE) std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::vector<std::pair<std::string*, Epoch>> kGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];


void
gc_records()
{
#ifdef KVS_Linux
  int core_pos = sched_getcpu();
  if (core_pos == -1) ERR;
  cpu_set_t current_mask = getThreadAffinity();
  setThreadAffinity(core_pos);
#endif
  std::mutex& mutex_for_gclist = kMutexGarbageRecords[core_pos];
  if (mutex_for_gclist.try_lock()) {
    auto itr = kGarbageRecords[core_pos].begin();
    while (itr != kGarbageRecords[core_pos].end()) {
      if ((*itr)->tidw.epoch <= loadAcquire(kReclamationEpoch)) {
        delete *itr;
        itr = kGarbageRecords[core_pos].erase(itr);
      } else {
        break;
      }
    }
    mutex_for_gclist.unlock();
  }
#ifdef KVS_Linux
  setThreadAffinity(current_mask);
#endif
}

} // namespace kvs
