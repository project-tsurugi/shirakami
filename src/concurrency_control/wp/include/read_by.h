/**
 * @file read_by.h
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2022-01-20
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "concurrency_control/wp/include/epoch.h"

namespace shirakami {

class read_by_bt {
public:
    using body_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using body_type = std::vector<body_elem_type>;

    /**
     * @brief Get the partial elements
     * @param epoch 
     * @return body_elem_type 
     */
    body_elem_type get(epoch::epoch_t epoch);

    /**
     * @brief gc
     * @pre get mtx_ lock
     * 
     */
    void gc();

    void push(body_elem_type elem);

private:
    std::mutex mtx_;

    /**
     * @brief body
     * @details std::pair.first is epoch. the second is batch_id.
     */
    body_type body_;
};

class read_by_occ {
public:
    using body_elem_type = epoch::epoch_t;
    using body_type = std::vector<body_elem_type>;

    /**
     * @brief gc
     * @pre get mtx_ lock
     * 
     */
    void gc();

    /**
     * @brief Get the partial elements
     * @param epoch 
     * @return true found  
     * @return false not found  
     */
    bool find(epoch::epoch_t epoch);

    epoch::epoch_t get_max_epoch() {
        return max_epoch_.load(std::memory_order_acquire);
    }

    void push(body_elem_type elem);

    void set_max_epoch(epoch::epoch_t const ep) {
        max_epoch_.store(ep, std::memory_order_release);
    }

private:
    std::atomic<std::uint64_t> max_epoch_{0};

    std::mutex mtx_;

    /**
     * @brief body
     * @details std::pair.first is epoch. the second is batch_id.
     */
    body_type body_;
};

} // namespace shirakami