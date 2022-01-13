/**
 * @file concurrency_control/wp/include/ongoing_tx.h
 */

#pragma once

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

    static void push(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        ids_.emplace_back(id);
    }

    static void remove(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        for (auto it = ids_.begin(); it != ids_.end(); ++it) {
            if (*it == id) {
                ids_.erase(it);
                return;
            }
        }
        LOG(FATAL);
    }


private:
    static inline std::mutex mtx_; // NOLINT
    static inline ids_type ids_;   // NOLINT
};

} // namespace shirakami