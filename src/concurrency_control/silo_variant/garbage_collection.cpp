/**
 * @file garbage_collection.cpp
 * @brief about garbage collection.
 */

#include "concurrency_control/silo_variant/include/garbage_collection.h"
#include "concurrency_control/silo_variant/include/session_info_table.h"

#ifdef INDEX_KOHLER_MASSTREE

#include "index/masstree_beta/include/masstree_beta_wrapper.h"

#endif

#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant::garbage_collection {

[[maybe_unused]] void release_all_heap_objects() {
    remove_all_leaf_from_mt_db_and_release();
    std::vector<std::thread> thv;
    for (auto &&elem : session_info_table::get_thread_info_table()) {
        thv.emplace_back([&elem] {
            // delete all garbage records
            for (auto &&r : elem.get_gc_record_container()) {
                delete r; // NOLINT
            }
            elem.get_gc_record_container().clear();
            // delete all garbage values
            for (auto &&v : elem.get_gc_value_container()) {
                delete v.first; // NOLINT
            }
            elem.get_gc_value_container().clear();
            // delete all garbage snap
            for (auto &&s : elem.get_gc_snap_cont()) {
                delete s.second; // NOLINT
            }
            elem.get_gc_snap_cont().clear();
        });
    }
    for (auto &&th : thv) th.join();
}

void remove_all_leaf_from_mt_db_and_release() {
#ifdef INDEX_KOHLER_MASSTREE
    std::vector<const Record*> scan_res;
    kohler_masstree::get_mtdb().scan("", scan_endpoint::INF, "", scan_endpoint::INF, &scan_res, false);  // NOLINT
    for (auto &&itr : scan_res) {
        std::string_view key_view = itr->get_tuple().get_key();
        kohler_masstree::get_mtdb().remove_value(key_view.data(), key_view.size());
        delete itr;  // NOLINT
    }

    /**
     * check whether index_kohler_masstree::get_mtdb() is empty.
     */
    scan_res.clear();
    kohler_masstree::get_mtdb().scan("", scan_endpoint::INF, "", scan_endpoint::INF, &scan_res, false);  // NOLINT
    if (!scan_res.empty()) std::abort();
#elif INDEX_YAKUSHIMA
    yakushima::destroy();
#endif
}

}  // namespace shirakami::cc_silo_variant::garbage_collection
