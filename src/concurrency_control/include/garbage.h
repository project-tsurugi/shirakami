#pragma once

#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

#include "concurrent_queue.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"

#include "shirakami/storage_options.h"

#include "glog/logging.h"

namespace shirakami::garbage {

using stats_info_type =
        std::vector<std::tuple<Storage, std::size_t, std::size_t, std::size_t,
                               std::size_t>>;
// background thread
//================================================================================
/**
 * @brief executor of garbage collection for versions and index.
 * @details Perform version pruning of GC based on the timestamp determined 
 * by the manager.
 */
[[maybe_unused]] inline std::thread cleaner; // NOLINT

/**
 * @brief garbage collection manager thread.
 * @details Periodically calculate @a min_step_epoch and @a min_batch_epoch.
 */
[[maybe_unused]] inline std::thread manager; // NOLINT

// function for background thread
[[maybe_unused]] void work_manager();

[[maybe_unused]] void work_cleaner();

// invoker for bg threads
[[maybe_unused]] static void invoke_bg_threads() {
    manager = std::thread(work_manager);
    cleaner = std::thread(work_cleaner);
}

// flags for background thread
[[maybe_unused]] inline std::atomic<bool> flag_cleaner_end{false};

[[maybe_unused]] inline std::atomic<bool> flag_manager_end{false};

// parameters for background thread
/**
 * @brief thread size of cleaner.
 */
[[maybe_unused]] inline std::size_t cleaner_thd_size{0};

// mutex for background thread
/**
 * @brief mutex for operation about cleaner.
 */
[[maybe_unused]] inline std::mutex mtx_cleaner_{};

// statistical data
/**
  * @brief Record the number of gc versions.
  */
[[maybe_unused]] inline std::atomic<std::uint64_t> gc_ct_ver_{0};


// container for gc
/**
 * @brief container of records which was unhooked from index.
 * First of elements is pointer to record. Second of elements is global epoch 
 * of unhooking.
 */
[[maybe_unused]] inline std::vector< // NOLINT
        std::pair<Record*, epoch::epoch_t>>
        container_rec_{};

// setter
[[maybe_unused]] static void set_flag_cleaner_end(bool const tf) {
    flag_cleaner_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_flag_manager_end(bool const tf) {
    flag_manager_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_cleaner_thd_size(std::size_t const n) {
    cleaner_thd_size = n;
}

// getter
[[maybe_unused]] static std::vector<std::pair<Record*, epoch::epoch_t>>&
get_container_rec() {
    return container_rec_;
}

[[maybe_unused]] static bool get_flag_cleaner_end() {
    return flag_cleaner_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_flag_manager_end() {
    return flag_manager_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static std::mutex& get_mtx_cleaner() { return mtx_cleaner_; }

[[maybe_unused]] static std::size_t get_cleaner_thd_size() {
    return cleaner_thd_size;
}

[[maybe_unused]] static std::atomic<std::uint64_t>& get_gc_ct_ver() {
    return gc_ct_ver_;
}

// join about bg threads
[[maybe_unused]] static void join_bg_threads() {
    manager.join();
    cleaner.join();
}

//================================================================================

// timestamps
//================================================================================
/**
 * @brief The minimum epoch in which a valid transitional step has been 
 * performed on all worker threads.
 */
inline std::atomic<epoch::epoch_t> min_step_epoch_{epoch::initial_epoch};

/**
 * @brief The minimum epoch of a batch transaction running on all worker 
 * threads.
 */
inline std::atomic<epoch::epoch_t> min_batch_epoch_{epoch::initial_epoch};

// getter
[[maybe_unused]] static epoch::epoch_t get_min_step_epoch() {
    return min_step_epoch_.load(std::memory_order_acquire);
}

[[maybe_unused]] static epoch::epoch_t get_min_batch_epoch() {
    return min_batch_epoch_.load(std::memory_order_acquire);
}

// setter
[[maybe_unused]] static void set_min_step_epoch(epoch::epoch_t e) {
    min_step_epoch_.store(e, std::memory_order_release);
}

[[maybe_unused]] static void set_min_batch_epoch(epoch::epoch_t e) {
    min_batch_epoch_.store(e, std::memory_order_release);
}

//================================================================================

// function for init / fin
//================================================================================
[[maybe_unused]] extern void init();

[[maybe_unused]] extern void fin();
//================================================================================

} // namespace shirakami::garbage