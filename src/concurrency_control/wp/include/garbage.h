#pragma once

#include <thread>

#include "concurrent_queue.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/version.h"

#include "glog/logging.h"

namespace shirakami::garbage {

// background thread
//================================================================================
/**
 * @brief executor of garbage collection for values.
 * @details Perform a GC based on the time stamp determined by the manager.
 */
[[maybe_unused]] inline std::thread value_cleaner; // NOLINT

/**
 * @brief executor of garbage collection for versions.
 * @details Perform a GC based on the time stamp determined by the manager.
 */
[[maybe_unused]] inline std::thread version_cleaner; // NOLINT

/**
 * @brief garbage collection manager thread.
 * @details Periodically calculate @a min_step_epoch and @a min_batch_epoch.
 */
[[maybe_unused]] inline std::thread manager; // NOLINT

// function for background thread
[[maybe_unused]] void work_manager();

[[maybe_unused]] void work_value_cleaner();

[[maybe_unused]] void work_version_cleaner();

// invoker for bg threads
[[maybe_unused]] static void invoke_bg_threads() {
    manager = std::thread(work_manager);
    value_cleaner = std::thread(work_value_cleaner);
    version_cleaner = std::thread(work_version_cleaner);
}

// flags for background thread
[[maybe_unused]] inline std::atomic<bool> flag_value_cleaner_end{false};

[[maybe_unused]] inline std::atomic<bool> flag_version_cleaner_end{false};

[[maybe_unused]] inline std::atomic<bool> flag_manager_end{false};

// setter
[[maybe_unused]] static void set_flag_value_cleaner_end(bool tf) {
    flag_value_cleaner_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_flag_version_cleaner_end(bool tf) {
    flag_version_cleaner_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_flag_manager_end(bool tf) {
    flag_manager_end.store(tf, std::memory_order_release);
}

// getter
[[maybe_unused]] static bool get_flag_value_cleaner_end() {
    return flag_value_cleaner_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_flag_version_cleaner_end() {
    return flag_version_cleaner_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_flag_manager_end() {
    return flag_manager_end.load(std::memory_order_acquire);
}

// join about bg threads
[[maybe_unused]] static void join_bg_threads() {
    manager.join();
    value_cleaner.join();
    version_cleaner.join();
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

class gc_handle {
public:
    /**
     * @brief value_type
     * @details The first element is the payload. The second element represents 
     * the epoch when the payload changes from global to invisible.
     */
    using value_type = std::pair<std::string*, epoch::epoch_t>;
    static constexpr value_type initial_value{nullptr, 0};

    void destroy();

    static std::atomic<std::uint64_t>& get_gc_ct_val() { return gc_ct_val_; }

    static std::atomic<std::uint64_t>& get_gc_ct_ver() { return gc_ct_ver_; }

    value_type get_val_cache() { return val_cache_; }

    shirakami::concurrent_queue<value_type>& get_val_cont() {
        return val_cont_;
    }

    void push_value(value_type const g_val) { val_cont_.push(g_val); }

    void set_val_cache(value_type const v) { val_cache_ = v; }

    bool val_cache_is_empty() {
        return val_cache_ ==
               std::pair<std::string*, epoch::epoch_t>(nullptr, 0);
    }

private:
    value_type val_cache_{initial_value};

    shirakami::concurrent_queue<value_type> val_cont_;

    // for measurement
    //==============================
    /**
     * @brief Record the number of gc vals.
     */
    static inline std::atomic<std::uint64_t> gc_ct_val_{0};

    /**
     * @brief Record the number of gc versions.
     */
    static inline std::atomic<std::uint64_t> gc_ct_ver_{0};

    //==============================
};

} // namespace shirakami::garbage