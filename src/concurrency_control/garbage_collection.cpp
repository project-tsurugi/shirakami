/**
 * @file garbage_collection.cpp
 * @brief about garbage collection.
 */

#include "concurrency_control/include/garbage_collection.h"
#include "concurrency_control/include/session_info_table.h"

#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami::garbage_collection {

[[maybe_unused]] void release_all_heap_objects() {
    remove_all_leaf_from_mt_db_and_release();
    std::vector<std::thread> thv;
    thv.reserve(session_info_table::get_thread_info_table().size());
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
    yakushima::destroy();
}

}  // namespace shirakami::garbage_collection
