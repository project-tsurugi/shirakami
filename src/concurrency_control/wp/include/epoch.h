/**
 * @file wp/include/epoch.h
 */

#pragma once

#include <atomic>
#include <thread>

namespace shirakami::epoch {

using epoch_t = std::int64_t;

[[maybe_unused]] inline std::atomic<epoch_t> global_epoch{0}; // NOLINT

[[maybe_unused]] inline std::atomic<epoch_t> reclamation_epoch{-2}; // NOLINT

[[maybe_unused]] inline std::thread epoch_thread; // NOLINT

[[maybe_unused]] inline std::atomic<bool> epoch_thread_end;        // NOLINT

[[maybe_unused]] static bool get_epoch_thread_end() { // NOLINT
    return epoch_thread_end.load(std::memory_order_acquire);
}

[[maybe_unused]] static epoch_t get_global_epoch() { // NOLINT
    return global_epoch.load(std::memory_order_acquire);
}

[[maybe_unused]] static epoch_t get_reclamation_epoch() { // NOLINT
    return reclamation_epoch.load(std::memory_order_acquire);
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

[[maybe_unused]] static void set_reclamation_epoch(const epoch_t epo) { // NOLINT
    reclamation_epoch.store(epo, std::memory_order_release);
}

[[maybe_unused]] extern bool check_epoch_loaded(); // NOLINT

[[maybe_unused]] extern void epoch_thread_work();

[[maybe_unused]] extern void invoke_epoch_thread();

} // namespace shirakami::epoch