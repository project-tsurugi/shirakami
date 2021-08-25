/**
 * @file cleanup_manager.h
 * @brief header about cleanup manager
 */

#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "concurrent_queue.h"
#include "epoch.h"
#include "record.h"

namespace shirakami::cleanup_manager {

// about manager thread
[[maybe_unused]] inline std::thread cleanup_manager_thread; // NOLINT
inline std::atomic<bool> cleanup_manager_thread_end;        // NOLINT

/**
 * @brief cleanup manager thread
 */
[[maybe_unused]] extern void cleanup_manager_func();

[[maybe_unused]] static void set_cleanup_manager_thread_end(const bool tf) {
    cleanup_manager_thread_end.store(tf, std::memory_order_release);
}

[[maybe_unused]] static void invoke_cleanup_manager() {
    set_cleanup_manager_thread_end(false);
    cleanup_manager_thread = std::thread(cleanup_manager_func);
}

[[maybe_unused]] static void join_cleanup_manager_thread() {
    cleanup_manager_thread.join();
}

class cleanup_handler {
public:
    using value_type = std::pair<std::string, Record*>;

    void cache(const value_type& val) {
        cache_ = val;
    }

    void clear() {
        cache_ = {};
        cont_.clear();
    }

    value_type& get_cache() {
        return cache_;
    }

    concurrent_queue<value_type>& get_cont() {
        return cont_;
    }

    void push(const value_type& val) {
        cont_.push(val);
    }

    bool try_pop(value_type& out) {
        return cont_.try_pop(out);
    }

private:
    value_type cache_;

    // Elements in this container will be removed from index
    // pair.first is storage, pair.second is pointer to record.
    concurrent_queue<value_type> cont_; // NOLINT
};

} // namespace shirakami::cleanup_manager
