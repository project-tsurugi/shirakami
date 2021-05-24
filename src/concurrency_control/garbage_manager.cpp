/**
 * @file garbage_collection.cpp
 * @brief about garbage collection.
 */

#include "clock.h"

#include "concurrency_control/include/garbage_manager.h"
#include "concurrency_control/include/session_info_table.h"
#include "concurrency_control/include/snapshot_manager.h"

#include "tuple_local.h" // sizeof(Tuple)

namespace shirakami::garbage_manager {

void gc_handler::clear_rec() {
    auto*& cache_rec = get_cache_rec();
    if (cache_rec != nullptr) {
        delete cache_rec; // NOLINT
        cache_rec = {};
    }
    auto& rec_cont = get_rec_cont();
    while (!rec_cont.empty()) {
        rec_cont.try_pop(cache_rec);
        if (cache_rec != nullptr) {
            delete cache_rec; // NOLINT
            cache_rec = {};
        }
    }
    rec_cont.clear();
}

void gc_handler::clear_val() {
    auto& cache_val = get_cache_val();
    if (cache_val.first != nullptr) {
        delete cache_val.first; // NOLINT
        cache_val = {};
    }
    auto& val_cont = get_val_cont();
    while (!val_cont.empty()) {
        val_cont.try_pop(cache_val);
        if (cache_val.first != nullptr) {
            delete cache_val.first; // NOLINT
            cache_val = {};
        }
    }
    val_cont.clear();
}

void gc_handler::clear_snap() {
    auto& cache_snap = get_cache_snap();
    if (cache_snap.second != nullptr) {
        delete cache_snap.second; // NOLINT
        cache_snap = {};
    }
    auto& snap_cont = get_snap_cont();
    while (!snap_cont.empty()) {
        snap_cont.try_pop(cache_snap);
        if (cache_snap.second != nullptr) {
            delete cache_snap.second; // NOLINT
            cache_snap = {};
        }
    }
}

void gc_handler::gc_rec() {
    auto& cont = get_rec_cont();
    auto*& cache = get_cache_rec();

    while (!cont.empty() || cache != nullptr) {
        if (cache == nullptr && !cont.try_pop(cache)) {
            break; // nothing
        }
        // cache is not null

        if (cache->get_tidw().get_epoch() <= epoch::get_reclamation_epoch()) {
#ifdef CPR
            if (cache->get_version() > cpr::global_phase_version::get_gpv().get_version()) {
                break;
            }
#endif
            delete cache;
            cache = {};
        } else {
            break;
        }
    }
}

void gc_handler::gc_val() {
    auto& cont = get_val_cont();
    auto& cache = get_cache_val();

    while (!cont.empty() || cache.first != nullptr) {
        if (cache.first == nullptr && !cont.try_pop(cache)) {
            break; // nothing
        }
        // cache is not null

        if (cache.second < epoch::get_reclamation_epoch()) {
            delete cache.first;
            cache = {};
        } else {
            break;
        }
    }
}

void gc_handler::gc_snap() {
    auto& cont = get_snap_cont();
    auto& cache = get_cache_snap();

    while (!cont.empty() || cache.second != nullptr) {
        if (cache.second == nullptr && !cont.try_pop(cache)) {
            break; // nothing
        }
        // cache is not null

        epoch::epoch_t ce = epoch::kGlobalEpoch.load(std::memory_order_acquire);
        epoch::epoch_t maybe_smallest_e = ce - 1;
        if (snapshot_manager::get_snap_epoch(cache.first + snapshot_manager::snapshot_epoch_times) <= snapshot_manager::get_snap_epoch(maybe_smallest_e)) {
            delete cache.second;
            cache = {};
        } else {
            break;
        }
    }
}

[[maybe_unused]] void release_all_heap_objects() {
    remove_all_leaf_from_mt_db_and_release();
    std::vector<std::thread> thv;
    thv.reserve(session_info_table::get_thread_info_table().size());
    for (auto&& elem : session_info_table::get_thread_info_table()) {
        auto& gc_handle = elem.get_gc_handle();
        auto process = [&gc_handle]() {
            gc_handle.clear();
        };
        if (gc_handle.get_rec_cont().size() > 1000 || gc_handle.get_val_cont().size() > 1000 || gc_handle.get_snap_cont().size() > 1000) { // NOLINT
            // Considering clean up time of test and benchmark.
            thv.emplace_back(process);
        } else {
            process();
        }
    }
    for (auto&& th : thv) th.join();
}

void remove_all_leaf_from_mt_db_and_release() {
    yakushima::destroy();
}

void garbage_manager_func() {
    while (!garbage_manager_thread_end.load(std::memory_order_acquire)) {
        sleepMs(PARAM_EPOCH_TIME);

        for (auto&& elem : session_info_table::get_thread_info_table()) {
            auto& handle = elem.get_gc_handle();
            handle.gc();
            if (garbage_manager_thread_end.load(std::memory_order_acquire)) break;
        }
    }
}
} // namespace shirakami::garbage_manager
