/**
 * @file garbage_collection.cpp
 * @brief about garbage collection.
 */

#include "cc/silo_variant/include/garbage_collection.h"

#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif
#include "cc/silo_variant/include/thread_info.h"
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

[[maybe_unused]] void garbage_collection::release_all_heap_objects() {
  garbage_collection::remove_all_leaf_from_mt_db_and_release();
  garbage_collection::delete_all_garbage_records();
  garbage_collection::delete_all_garbage_values();
}

void garbage_collection::remove_all_leaf_from_mt_db_and_release() {
#ifdef INDEX_KOHLER_MASSTREE
  std::vector<const Record*> scan_res;
  kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                   &scan_res, false);  // NOLINT
  for (auto&& itr : scan_res) {
    std::string_view key_view = itr->get_tuple().get_key();
    kohler_masstree::get_mtdb().remove_value(key_view.data(), key_view.size());
    delete itr;  // NOLINT
  }

  /**
   * check whether index_kohler_masstree::get_mtdb() is empty.
   */
  scan_res.clear();
  kohler_masstree::get_mtdb().scan(nullptr, 0, false, nullptr, 0, false,
                                   &scan_res, false);  // NOLINT
  if (!scan_res.empty()) std::abort();
#elif INDEX_YAKUSHIMA
  yakushima::yakushima_kvs::destroy();
#endif
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
    std::mutex& mutex_for_gc_list =
        garbage_collection::get_mutex_garbage_records_at(
            this->gc_handle_.get_container_index());
    if (mutex_for_gc_list.try_lock()) {
      auto itr = this->gc_handle_.get_record_container()->begin();
      while (itr != this->gc_handle_.get_record_container()->end()) {
        if ((*itr)->get_tidw().get_epoch() <= epoch::get_reclamation_epoch()) {
          delete *itr;  // NOLINT
          itr = this->gc_handle_.get_record_container()->erase(itr);
        } else {
          break;
        }
      }
      mutex_for_gc_list.unlock();
    }
  }
  // for values
  {
    std::mutex& mutex_for_gc_list =
        garbage_collection::get_mutex_garbage_values_at(
            this->gc_handle_.get_container_index());
    if (mutex_for_gc_list.try_lock()) {
      auto itr = this->gc_handle_.get_value_container()->begin();
      while (itr != this->gc_handle_.get_value_container()->end()) {
        if (itr->second <= epoch::get_reclamation_epoch()) {
          delete itr->first;  // NOLINT
          itr = this->gc_handle_.get_value_container()->erase(itr);
        } else {
          break;
        }
      }
      mutex_for_gc_list.unlock();
    }
  }
}

}  // namespace shirakami::silo_variant
