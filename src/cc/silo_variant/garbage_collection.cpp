/**
 * @file garbage_collection.cpp
 * @brief about garbage collection.
 */

#include "cc/silo_variant/include/garbage_collection.h"

#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#elif INDEX_YAKUSHIMA
#include "yakushima/include/kvs.h"
#endif
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant::garbage_collection {

void delete_all_garbage_records() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto&& itr : get_garbage_records_at(i)) {
      delete itr;  // NOLINT
    }
    get_garbage_records_at(i).clear();
  }
}

void delete_all_garbage_values() {
  for (auto i = 0; i < KVS_NUMBER_OF_LOGICAL_CORES; ++i) {
    for (auto&& itr : get_garbage_values_at(i)) {
      delete itr.first;  // NOLINT
    }
    get_garbage_values_at(i).clear();
  }
}

[[maybe_unused]] void release_all_heap_objects() {
  remove_all_leaf_from_mt_db_and_release();
  delete_all_garbage_records();
  delete_all_garbage_values();
}

void remove_all_leaf_from_mt_db_and_release() {
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
  yakushima::destroy();
#endif
}

}  // namespace shirakami::cc_silo_variant::garbage_collection
