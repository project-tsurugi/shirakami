#pragma once

#include <mutex>
#include <thread>

#include "concurrent_queue.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/version.h"

#include "glog/logging.h"

namespace shirakami::garbage {

// background thread
//================================================================================
/**
 * @brief executor of garbage collection for versions.
 * @details Perform version pruning of GC based on the timestamp determined 
 * by the manager.
 */
[[maybe_unused]] inline std::thread version_cleaner; // NOLINT

/**
 * @brief executor of garbage collection for unhook page from index.
 * @details Perform unhook operation of GC based on the timestamp determined 
 * by the manager.
 */
[[maybe_unused]] inline std::thread unhook_cleaner; // NOLINT

/**
 * @brief garbage collection manager thread.
 * @details Periodically calculate @a min_step_epoch and @a min_batch_epoch.
 */
[[maybe_unused]] inline std::thread manager; // NOLINT

// function for background thread
[[maybe_unused]] void work_manager();

[[maybe_unused]] void work_version_cleaner();

[[maybe_unused]] void work_unhook_cleaner();

// invoker for bg threads
[[maybe_unused]] static void invoke_bg_threads() {
    manager = std::thread(work_manager);
    version_cleaner = std::thread(work_version_cleaner);
    unhook_cleaner = std::thread(work_unhook_cleaner);
}

// flags for background thread
[[maybe_unused]] inline std::atomic<bool> flag_version_cleaner_end{false};

[[maybe_unused]] inline std::atomic<bool> flag_unhook_cleaner_end{false};

[[maybe_unused]] inline std::atomic<bool> flag_manager_end{false};

// parameters for background thread
/**
 * @brief thread size of version cleaner (pruner).
 */
[[maybe_unused]] inline std::size_t ver_cleaner_thd_size{0};

/**
 * @brief thread size of unhook cleaner.
 */
[[maybe_unused]] inline std::size_t unhook_cleaner_thd_size{0};

// mutex for background thread
/**
 * @brief mutex for operation about version cleaner.
 */
inline std::mutex mtx_version_cleaner_{};

/**
 * @brief mutex for operation about unhook cleaner.
 */
inline std::mutex mtx_unhook_cleaner_{};

// statistical data
/**
  * @brief Record the number of gc versions.
  */
inline std::atomic<std::uint64_t> gc_ct_ver_{0};

// setter
[[maybe_unused]] static void set_flag_unhook_cleaner_end(bool const tf) {
    flag_unhook_cleaner_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_flag_version_cleaner_end(bool const tf) {
    flag_version_cleaner_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_flag_manager_end(bool const tf) {
    flag_manager_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_ver_cleaner_thd_size(std::size_t const n) {
    ver_cleaner_thd_size = n;
}

[[maybe_unused]] static void set_unhook_cleaner_thd_size(std::size_t const n) {
    unhook_cleaner_thd_size = n;
}

// getter
[[maybe_unused]] static bool get_flag_unhook_cleaner_end() {
    return flag_unhook_cleaner_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_flag_version_cleaner_end() {
    return flag_version_cleaner_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_flag_manager_end() {
    return flag_manager_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static std::mutex& get_mtx_unhook_cleaner() {
    return mtx_unhook_cleaner_;
}

[[maybe_unused]] static std::mutex& get_mtx_version_cleaner() {
    return mtx_version_cleaner_;
}

[[maybe_unused]] static std::size_t get_unhook_cleaner_thd_size() {
    return unhook_cleaner_thd_size;
}

[[maybe_unused]] static std::size_t get_ver_cleaner_thd_size() {
    return ver_cleaner_thd_size;
}

[[maybe_unused]] static std::atomic<std::uint64_t>& get_gc_ct_ver() {
    return gc_ct_ver_;
}

// join about bg threads
[[maybe_unused]] static void join_bg_threads() {
    manager.join();
    version_cleaner.join();
    unhook_cleaner.join();
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