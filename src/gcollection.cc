/**
 * @file gcollection.cc
 * @brief about garbage collection.
 */

#include <iostream>
#include <utility>

#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "epoch.hh"
#include "gcollection.hh"
#include "scheme.hh"

using std::cout, std::endl; 

namespace kvs {

alignas(CACHE_LINE_SIZE) std::vector<Record*> kGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::mutex kMutexGarbageRecords[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::vector<std::pair<std::string*, Epoch>> kGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];
alignas(CACHE_LINE_SIZE) std::mutex kMutexGarbageValues[KVS_NUMBER_OF_LOGICAL_CORES];

void
ThreadInfo::gc_records_and_values()
{
  // for records
  {
    std::mutex& mutex_for_gclist = kMutexGarbageRecords[this->gc_container_index_];
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_record_container_->begin();
      while (itr != this->gc_record_container_->end()) {
        if ((*itr)->get_tidw().get_epoch() <= loadAcquire(kReclamationEpoch)) {
#if 0
          cout << "gc record : key : " <<  (*itr)->get_tuple().get_key() << endl;
          cout << "gc record : epoch : " <<  (*itr)->get_tidw().get_epoch() << endl;
          cout << "kReclamationEpoch : " << loadAcquire(kReclamationEpoch) << endl;
#endif
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
    std::mutex& mutex_for_gclist = kMutexGarbageValues[this->gc_container_index_];
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_value_container_->begin();
      while (itr != this->gc_value_container_->end()) {
        if (itr->second <= loadAcquire(kReclamationEpoch)) {
#if 0
          cout << "gc record : value : " <<  *itr->first << endl;
          cout << "gc record : epoch : " <<  itr->second << endl;
          cout << "kReclamationEpoch : " << loadAcquire(kReclamationEpoch) << endl;
#endif
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

} // namespace kvs
