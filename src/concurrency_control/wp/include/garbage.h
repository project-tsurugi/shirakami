#pragma once

#include <thread>

#include "concurrent_queue.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/version.h"

namespace shirakami::garbage {

// background thread
//================================================================================
/**
 * @brief executor of garbage collection.
 * @details Perform a GC based on the time stamp determined by the manager.
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

[[maybe_unused]] static void join_bg_threads() {
    manager.join();
    cleaner.join();
}

//================================================================================

// flags for background thread
//================================================================================
[[maybe_unused]] inline std::atomic<bool> flag_cleaner_end{false};

[[maybe_unused]] inline std::atomic<bool> flag_manager_end{false};

// setter
[[maybe_unused]] static void set_flag_cleaner_end(bool tf) {
    flag_cleaner_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_flag_manager_end(bool tf) {
    flag_manager_end.store(tf, std::memory_order_release);
}

// getter
[[maybe_unused]] static bool get_flag_cleaner_end() {
    return flag_cleaner_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static bool get_flag_manager_end() {
    return flag_manager_end.load(std::memory_order_acquire);
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

[[maybe_unused]] static epoch::epoch_t get_batch_epoch() {
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
    using value_type = std::pair<std::string*, epoch::epoch_t>;
    using version_type = std::pair<version*, epoch::epoch_t>;

    void destroy();

    void push_value(value_type g_val) { val_cont_.push(g_val); }

private:
    shirakami::concurrent_queue<value_type> val_cont_;
};

} // namespace shirakami::garbage