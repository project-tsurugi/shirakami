/**
 * @file concurrency_control/wp/include/ongoing_tx.h
 */

#pragma once

#include <atomic>
#include <mutex>

#include "glog/logging.h"

namespace shirakami {

class ongoing_tx {
public:
    using ids_type = std::vector<std::size_t>;

    static bool exist(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        for (auto&& elem : ids_) {
            if (elem == id) { return true; }
        }
        return false;
    }

    static bool exist_preceding(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        for (auto&& elem : ids_) {
            if (elem < id) { return true; }
        }
        return false;
    }

    /**
     * @brief Get the lowest id object
     * 
     * @return std::size_t if ret equals to 0, ids_ is empty. Otherwise, ret is lowest id.
     */
    static std::size_t get_lowest_id() {
        return lowest_id_.load(std::memory_order_acquire);
    }

    static void push(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        if (ids_.empty()) { set_lowest_id(id); }
        ids_.emplace_back(id);
    }

    static void remove(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        std::size_t lowest_id{0};
        bool first{true};
        bool erased{false};
        for (auto it = ids_.begin(); it != ids_.end();) {
            // update lowest id 
            if (first) {
                lowest_id = *it;
                first = false;
            } else {
                if (*it < lowest_id) { lowest_id = *it; }
            }

            if (*it == id) {
                ids_.erase(it);
                erased = true;
            } else {
                ++it;
            }
        }
        set_lowest_id(lowest_id);
        if (!erased) { LOG(FATAL); }
    }

    static void set_lowest_id(std::size_t id) {
        lowest_id_.store(id, std::memory_order_release);
    }

private:
    static inline std::mutex mtx_;                        // NOLINT
    static inline ids_type ids_;                          // NOLINT
    static inline std::atomic<std::size_t> lowest_id_{0}; // NOLINT
};

} // namespace shirakami