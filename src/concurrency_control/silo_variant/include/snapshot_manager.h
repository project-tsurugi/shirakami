/**
 * @file snapshot_manager.h
 * @brief header about snapshot manager
 */

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrent_queue.h"
#include "epoch.h"
#include "record.h"

namespace shirakami::snapshot_manager
{
/**
 * @details 25 epoch equals to 1 snapshot epoch.
 */
constexpr size_t snapshot_epoch_times = PARAM_SNAPSHOT_EPOCH;

// Elements in this container will be removed from index
// todo, enhancement  : It will use oneTBB container.
[[maybe_unused]] inline concurrent_queue<std::pair<std::string, Record*>> remove_rec_cont;  // NOLINT

// about epoch thread
[[maybe_unused]] inline std::thread snapshot_manager_thread;  // NOLINT
inline std::atomic<bool> snapshot_manager_thread_end;         // NOLINT

[[maybe_unused]] static epoch::epoch_t get_snap_epoch(epoch::epoch_t epo)
{  // NOLINT
    return snapshot_epoch_times * (epo / snapshot_epoch_times);
}

/**
 * @brief snapshot manager thread
 */
[[maybe_unused]] extern void snapshot_manager_func();

[[maybe_unused]] static void set_snapshot_manager_thread_end(const bool tf)
{
    snapshot_manager_thread_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void invoke_snapshot_manager()
{
    set_snapshot_manager_thread_end(false);
    snapshot_manager_thread = std::thread(snapshot_manager_func);
}

[[maybe_unused]] static void join_snapshot_manager_thread()
{
    snapshot_manager_thread.join();
}

}  // namespace shirakami::cc_silo_variant::snapshot_manager
