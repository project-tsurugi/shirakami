/**
 * @file gcollection.cc
 * @brief about garbage collection.
 */

#include "garbage_collection.h"

#include "index.h"
#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

[[maybe_unused]] void garbage_collection::release_all_heap_objects() {
  garbage_collection::remove_all_leaf_from_mtdb_and_release();
  garbage_collection::delete_all_garbage_records();
  garbage_collection::delete_all_garbage_values();
}

void garbage_collection::remove_all_leaf_from_mtdb_and_release() {
  std::vector<const Record*> scan_res;
  index_kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                         &scan_res, false);  // NOLINT

  for (auto&& itr : scan_res) {
    std::string_view key_view = itr->get_tuple().get_key();
    index_kohler_masstree::get_mtdb().remove_value(key_view.data(),
                                                   key_view.size());
    delete itr;  // NOLINT
  }

  /**
   * check whether index_kohler_masstree::get_mtdb() is empty.
   */
  scan_res.clear();
  index_kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                         &scan_res, false);  // NOLINT
  if (!scan_res.empty()) std::abort();
}

void garbage_collection::delete_all_garbage_records() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto&& itr : get_garbage_records_at(i)) {
      delete itr;  // NOLINT
    }
    get_garbage_records_at(i).clear();
  }
}

void garbage_collection::delete_all_garbage_values() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto&& itr : get_garbage_values_at(i)) {
      delete itr.first;  // NOLINT
    }
    get_garbage_values_at(i).clear();
  }
}

void ThreadInfo::gc_records_and_values() const {
  // for records
  {
    std::mutex& mutex_for_gclist =
        garbage_collection::get_mutex_garbage_records_at(
            this->gc_handle_.get_container_index());
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_handle_.get_record_container()->begin();
      while (itr != this->gc_handle_.get_record_container()->end()) {
        if ((*itr)->get_tidw().get_epoch() <= epoch::get_reclamation_epoch()) {
          delete *itr;  // NOLINT
          itr = this->gc_handle_.get_record_container()->erase(itr);
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
        garbage_collection::get_mutex_garbage_values_at(
            this->gc_handle_.get_container_index());
    if (mutex_for_gclist.try_lock()) {
      auto itr = this->gc_handle_.get_value_container()->begin();
      while (itr != this->gc_handle_.get_value_container()->end()) {
        if (itr->second <= epoch::get_reclamation_epoch()) {
          delete itr->first;  // NOLINT
          itr = this->gc_handle_.get_value_container()->erase(itr);
        } else {
          break;
        }
      }
      mutex_for_gclist.unlock();
    }
  }
}

}  // namespace shirakami
