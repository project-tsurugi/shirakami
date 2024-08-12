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
#include <map>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_set.hpp>

#include "concurrency_control/include/epoch.h"

#include "shirakami/scheme.h"

namespace shirakami {

class point_read_by_long {
public:
    using body_elem_type = std::pair<epoch::epoch_t, std::size_t>;
    using body_type = std::vector<body_elem_type>;

    /**
     * @brief get equal epoch's read_by
     * @param[in] token
     * @return body_elem_type
     */
    bool is_exist(Token token);

    /**
     * @brief push element and gc.
     * @param[in] elem
     */
    void push(body_elem_type elem);

    /**
     * @brief show contents.
     */
    void print();

private:
    std::shared_mutex mtx_;

    /**
     * @brief body
     * @details std::pair.first is epoch. the second is long_tx_id.
     */
    body_type body_;
};

class stringInf {
public:
    // stringInf(stringInf&& obj) : inf_(obj.inf_), str_(std::move(obj.str_)) { }
    // stringInf(const stringInf& obj) : inf_(obj.inf_), str_(obj.str_) { }

    stringInf(std::string_view key) : inf_(0), str_(key) { }
    stringInf(std::string key) : inf_(0), str_(key) { }
    explicit stringInf(int inf) : inf_(inf) { }
    stringInf() : inf_(0) { }

    friend bool operator<(const stringInf& a, const stringInf& b) {
        if (a.inf_ != b.inf_) { return a.inf_ < b.inf_; }
        return a.str_ < b.str_;
    }
    friend bool operator==(const stringInf& a, const stringInf& b) {
        return a.inf_ == b.inf_ && a.str_ == b.str_;
    }

private:
    int inf_;
    std::string str_;
};

class range_read_by_long {
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
    // epoch -> ltx_id -> interval
    using body_type = std::map<std::size_t, std::map<std::size_t, boost::icl::interval_set<stringInf>>>;

    bool is_exist(epoch::epoch_t ep, std::size_t ltx_id, std::string_view key);

    /**
     * @brief push element and gc.
     * @param[in] elem
     */
    void push(body_elem_type const& elem);

private:
    std::mutex mtx_;
    body_type body_;
};

class point_read_by_short {
public:
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

    void push(epoch::epoch_t elem);

    void set_max_epoch(epoch::epoch_t const ep) {
        max_epoch_.store(ep, std::memory_order_release);
    }

private:
    std::atomic<epoch::epoch_t> max_epoch_{0};
};

class range_read_by_short {
public:
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

    void push(epoch::epoch_t elem);

    void set_max_epoch(epoch::epoch_t const ep) {
        max_epoch_.store(ep, std::memory_order_release);
    }

private:
    // firstly, it express range by 0 or 1.
    std::atomic<epoch::epoch_t> max_epoch_{0};
};

} // namespace shirakami
