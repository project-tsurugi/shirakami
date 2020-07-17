/**
 * @file gcollection.cc
 * @brief about garbage collection.
 */

#include <utility>

#include "gcollection.hh"

#include "atomic_wrapper.hh"
#include "epoch.hh"
#include "masstree_wrapper.hh"
#include "scheme.hh"
#include "tuple.hh"
#include "xact.hh"

namespace kvs {

alignas(CACHE_LINE_SIZE) std::array<
    std::vector<Record*>, KVS_NUMBER_OF_LOGICAL_CORES> kGarbageRecords{};
alignas(CACHE_LINE_SIZE)
    std::array<std::mutex, KVS_NUMBER_OF_LOGICAL_CORES> kMutexGarbageRecords{};
alignas(
    CACHE_LINE_SIZE) std::array<std::vector<std::pair<std::string*, Epoch>>,
                                KVS_NUMBER_OF_LOGICAL_CORES> kGarbageValues{};
alignas(CACHE_LINE_SIZE)
    std::array<std::mutex, KVS_NUMBER_OF_LOGICAL_CORES> kMutexGarbageValues{};

void release_all_heap_objects() {
  remove_all_leaf_from_mtdb_and_release();
  delete_all_garbage_records();
  delete_all_garbage_values();
}

void remove_all_leaf_from_mtdb_and_release() {
  std::vector<const Record*> scan_res;
  MTDB.scan(nullptr, 0, false, nullptr, 0, false, &scan_res, false);  // NOLINT

  for (auto&& itr : scan_res) {
    std::string_view key_view = itr->get_tuple().get_key();
    MTDB.remove_value(key_view.data(), key_view.size());
    delete itr;  // NOLINT
  }

  /**
   * check whether MTDB is empty.
   */
  scan_res.clear();
  MTDB.scan(nullptr, 0, false, nullptr, 0, false, &scan_res, false);  // NOLINT
  if (!scan_res.empty()) std::abort();
}

void delete_all_garbage_records() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto&& itr : kGarbageRecords.at(i)) {
      delete itr;  // NOLINT
    }
    kGarbageRecords.at(i).clear();
  }
}

void delete_all_garbage_values() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto&& itr : kGarbageValues.at(i)) {
      delete itr.first; // NOLINT
    }
    kGarbageValues.at(i).clear();
  }
}

void ThreadInfo::gc_records_and_values() const {
  // for records
  {
    std::mutex& mutex_for_gclist =
        kMutexGarbageRecords.at(this->gc_container_index_);
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_record_container_->begin();
      while (itr != this->gc_record_container_->end()) {
        if ((*itr)->get_tidw().get_epoch() <= loadAcquire(kReclamationEpoch)) {
          delete *itr; // NOLINT
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
        kMutexGarbageValues.at(this->gc_container_index_);
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_value_container_->begin();
      while (itr != this->gc_value_container_->end()) {
        if (itr->second <= loadAcquire(kReclamationEpoch)) {
          delete itr->first; // NOLINT
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
