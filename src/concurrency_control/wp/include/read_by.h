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

#include "shirakami/scheme.h"

namespace shirakami {

class point_read_by_bt {
public:
    using body_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using body_type = std::vector<body_elem_type>;

    /**
     * @brief get equal epoch's read_by
     * @param[in] epoch 
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

class range_read_by_bt {
public:
    /**
     * body element type
     * 0: long tx's epoch. 1: long tx's id. 2: left key. 3: left 
     * endpoint property. 4: right key. 5: right endpoint property.
     */
    static constexpr std::size_t index_epoch = 0;
    static constexpr std::size_t index_tx_id = 1;
    static constexpr std::size_t index_l_key = 2;
    static constexpr std::size_t index_l_ep = 3;
    static constexpr std::size_t index_r_key = 4;
    static constexpr std::size_t index_r_ep = 5;
    using body_elem_type =
            std::tuple<epoch::epoch_t, std::size_t, std::string, scan_endpoint,
                       std::string, scan_endpoint>;
    using body_type = std::vector<body_elem_type>;

    body_elem_type get(epoch::epoch_t ep, std::string_view key);

    void gc();

    void push(body_elem_type elem);

private:
    std::mutex mtx_;
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

    std::atomic<epoch::epoch_t>& get_max_epoch_ref() { return max_epoch_; }

    void push(body_elem_type elem);

    void set_max_epoch(epoch::epoch_t const ep) {
        max_epoch_.store(ep, std::memory_order_release);
    }

private:
    std::atomic<epoch::epoch_t> max_epoch_{0};

    std::mutex mtx_;

    /**
     * @brief body
     * @details std::pair.first is epoch. the second is batch_id.
     */
    body_type body_;
};

} // namespace shirakami