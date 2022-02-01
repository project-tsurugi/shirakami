/**
 * @file concurrency_control/silo/include/garbage_manager.h
 * @brief about garbage collection
 */

#pragma once

#include <array>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "concurrent_queue.h"
#include "cpu.h"
#include "epoch.h"
#include "record.h"

namespace shirakami::garbage_manager {

inline std::thread garbage_manager_thread;           // NOLINT
inline std::atomic<bool> garbage_manager_thread_end; // NOLINT

extern void garbage_manager_func();

static void set_garbage_manager_thread_end(const bool tf) {
    garbage_manager_thread_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void invoke_garbage_manager() {
    set_garbage_manager_thread_end(false);
    garbage_manager_thread = std::thread(garbage_manager_func);
}

[[maybe_unused]] static void join_garbage_manager_thread() {
    garbage_manager_thread.join();
}

class gc_handler {
public:
    using snap_type = std::pair<epoch::epoch_t, Record*>;

    void clear() {
        clear_rec();
        clear_snap();
    }

    void clear_rec();

    void clear_snap();

    void gc_rec();

    void gc_snap();

    void gc() {
        gc_rec();
        gc_snap();
    }

    Record*& get_cache_rec() { return cache_rec_; }

    snap_type& get_cache_snap() { return cache_snap_; }

    concurrent_queue<Record*>& get_rec_cont() { return rec_cont_; }

    concurrent_queue<snap_type>& get_snap_cont() { return snap_cont_; }

private:
    Record* cache_rec_{};
    concurrent_queue<Record*> rec_cont_;
    snap_type cache_snap_{};
    concurrent_queue<snap_type> snap_cont_;
};

/**
 * @brief Release all heap objects in this system.
 * @details Do three functions: delete_all_garbage_values(),
 * delete_all_garbage_records(), and remove_all_leaf_from_mt_db_and_release().
 * @pre This function should be called at terminating db.
 * @return void
 */
[[maybe_unused]] extern void release_all_heap_objects();

/**
 * @brief Remove all leaf nodes from MTDB and release those heap objects.
 * @pre This function should be called at terminating db.
 * @return void
 */
extern void remove_all_leaf_from_mt_db_and_release();

} // namespace shirakami::garbage_manager
