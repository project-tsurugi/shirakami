/**
 * @file gcollection.cc
 * @brief about garbage collection.
 */

#include <utility>

#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "epoch.hh"
#include "gcollection.hh"
#include "scheme.hh"

namespace kvs {

alignas(CACHE_LINE_SIZE)
    std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::mutex
    kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::vector<
    std::pair<std::string*, Epoch>> kGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::mutex
    kMutexGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];

void delete_all_garbage_records() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto itr = kGarbageRecords[i].begin(); itr != kGarbageRecords[i].end();
         ++itr) {
      delete *itr;
    }
    kGarbageRecords[i].clear();
  }
}

void delete_all_garbage_values() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto itr = kGarbageValues[i].begin(); itr != kGarbageValues[i].end();
         ++itr) {
      delete itr->first;
    }
    kGarbageValues[i].clear();
  }
}

void ThreadInfo::gc_records_and_values() {
  // for records
  {
    std::mutex& mutex_for_gclist =
        kMutexGarbageRecords[this->gc_container_index_];
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_record_container_->begin();
      while (itr != this->gc_record_container_->end()) {
        if ((*itr)->get_tidw().get_epoch() <= loadAcquire(kReclamationEpoch)) {
          delete *itr;
          itr = this->gc_record_container_->erase(itr);
        } else {
          break;
        }
      }
      mutex_for_gclist.unlock();
    }
  }
  // for values
  {
    std::mutex& mutex_for_gclist =
        kMutexGarbageValues[this->gc_container_index_];
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_value_container_->begin();
      while (itr != this->gc_value_container_->end()) {
        if (itr->second <= loadAcquire(kReclamationEpoch)) {
          delete itr->first;
          itr = this->gc_value_container_->erase(itr);
        } else {
          break;
        }
      }
      mutex_for_gclist.unlock();
    }
  }
}

}  // namespace kvs
