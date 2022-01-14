/**
 * @file concurrency_control/wp/include/ongoing_tx.h
 */

#pragma once

#include <atomic>
#include <mutex>

#include "concurrency_control/wp/include/epoch.h"

#include "glog/logging.h"

namespace shirakami {

class ongoing_tx {
public:
    /**
      * @brief tx_info_elem_type. first is epoch, second is batch id.
      * 
      */
    using tx_info_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using tx_info_type = std::vector<tx_info_elem_type>;

    static bool exist_id(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        for (auto&& elem : tx_info_) {
            if (elem.second == id) { return true; }
        }
        return false;
    }

    static bool exist_preceding_id(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        for (auto&& elem : tx_info_) {
            if (elem.second < id) { return true; }
        }
        return false;
    }

    /**
     * @brief Get the lowest epoch
     * 
     * @return std::size_t if ret equals to 0, tx_info_ is empty. Otherwise, ret is lowest epoch.
     */
    static epoch::epoch_t get_lowest_epoch() {
        return lowest_epoch_.load(std::memory_order_acquire);
    }

    static void push(tx_info_elem_type ti) {
        std::unique_lock<std::mutex> lk{mtx_};
        if (tx_info_.empty()) { set_lowest_epoch(ti.first); }
        tx_info_.emplace_back(ti);
    }

    static void remove_id(std::size_t id) {
        std::unique_lock<std::mutex> lk{mtx_};
        epoch::epoch_t lep{0};
        bool first{true};
        bool erased{false};
        for (auto it = tx_info_.begin(); it != tx_info_.end();) {
            // update lowest epoch
            if (first) {
                lep = (*it).first;
                first = false;
            } else {
                if ((*it).first < lep) { lep = (*it).first; }
            }

            if (!erased && (*it).second == id) {
                tx_info_.erase(it);
                erased = true;
            } else {
                ++it;
            }
        }
        set_lowest_epoch(lep);
        if (!erased) { LOG(FATAL); }
    }

    static void set_lowest_epoch(epoch::epoch_t ep) {
        lowest_epoch_.store(ep, std::memory_order_release);
    }

private:
    static inline std::mutex mtx_;                        // NOLINT
    static inline tx_info_type tx_info_;                          // NOLINT
    static inline std::atomic<epoch::epoch_t> lowest_epoch_{0}; // NOLINT
};

} // namespace shirakami