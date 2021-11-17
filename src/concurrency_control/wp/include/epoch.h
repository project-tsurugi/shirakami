/**
 * @file wp/include/epoch.h
 */

#pragma once

#include <atomic>
#include <thread>

namespace shirakami::epoch {

using epoch_t = std::int64_t;

static constexpr epoch_t initial_epoch{1};

static constexpr epoch_t max_epoch{INT64_MAX};

/**
 * @brief global epoch
 * @pre We start with 1 because we give 0 the meaning of uninitialized.
 */
[[maybe_unused]] inline std::atomic<epoch_t> global_epoch{initial_epoch}; // NOLINT

[[maybe_unused]] inline std::thread epoch_thread; // NOLINT

[[maybe_unused]] inline std::atomic<bool> epoch_thread_end;        // NOLINT

[[maybe_unused]] static bool get_epoch_thread_end() { // NOLINT
    return epoch_thread_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static epoch_t get_global_epoch() { // NOLINT
    return global_epoch.load(std::memory_order_acquire);
}

[[maybe_unused]] static void join_epoch_thread() { 
    epoch_thread.join();
}

[[maybe_unused]] static void set_epoch_thread_end(const bool tf) {
    epoch_thread_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void set_global_epoch(const epoch_t epo) { // NOLINT
    global_epoch.store(epo, std::memory_order_release);
}

} // namespace shirakami::epoch