/**
 * @file snapshot_manager.h
 * @brief header about snapshot manager
 */

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "epoch.h"
#include "record.h"

namespace shirakami::cc_silo_variant::snapshot_manager {

// Elements in this container will be removed from index
// todo, enhancement  : It will use oneTBB container.
[[maybe_unused]] inline std::vector<Record*> remove_rec_cont; // NOLINT
[[maybe_unused]] inline std::mutex remove_rec_cont_mutex; // NOLINT
// Memory used by elements in this container will be released.
[[maybe_unused]] inline std::vector<std::pair<epoch::epoch_t , Record*>> release_rec_cont; // NOLINT

// about epoch thread
[[maybe_unused]] inline std::thread snapshot_manager_thread;  // NOLINT
inline std::atomic<bool> snapshot_manager_thread_end;          // NOLINT

/**
 * @brief snapshot manager thread
 */
[[maybe_unused]] extern void snapshot_manager_func();

[[maybe_unused]] static void set_snapshot_manager_thread_end(const bool tf) {
    snapshot_manager_thread_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void invoke_snapshot_manager() {
    set_snapshot_manager_thread_end(false);
    snapshot_manager_thread = std::thread(snapshot_manager_func);
}

[[maybe_unused]] static void join_snapshot_manager_thread() { snapshot_manager_thread.join(); }

}  // namespace shirakami::cc_silo_variant::epoch
