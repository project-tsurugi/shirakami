/**
 * @file session.h
 */

#pragma once

#include <atomic>
#include <array>

#include "cpu.h"
#include "epoch.h"
#include "local_set.h"

#include "concurrency_control/silo/include/tid.h"

#include "yakushima/include/kvs.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) session {
public:
    using node_set_type = std::vector<std::pair<yakushima::node_version64_body,
                                                yakushima::node_version64*>>;

    tid_word get_mrc_tid() { return mrc_tid_; }

    bool get_tx_began() { return tx_began_.load(std::memory_order_acquire); }

    bool get_visible() { return visible_.load(std::memory_order_acquire); }

    void push_to_read_set(read_set_obj&& elem) {
        read_set_.emplace_back(std::move(elem));
    }

    void push_to_write_set(write_set_obj&& elem) {
        write_set_.push(std::move(elem));
    }

    void set_mrc_tid(tid_word tidw) { mrc_tid_ = tidw; }

    void set_visible(bool tf) { visible_.store(tf, std::memory_order_release); }

    void set_tx_began(bool tf) { tx_began_.store(tf, std::memory_order_release); }

private:
    /**
     * @brief most recently chosen tid for calculate new tid.
     */
    tid_word mrc_tid_{};

    /**
     * @brief If this is true, this session is live, otherwise, not live.
     */
    std::atomic<bool> visible_{false};

    /**
     * @brief Flag of transaction beginning.
     * @details If this is true, this session is in some tx, otherwise, not.
     */
    std::atomic<bool> tx_began_{false};

    /**
     * @brief local read set.
     */
    std::vector<read_set_obj> read_set_{};

    /**
     * @brief local write set.
     */
    local_write_set write_set_{};

    /**
     * about indexing
     */
    yakushima::Token yakushima_token_{};
    node_set_type node_set{};
};

class session_table {
public:

private:
    /**
     * @brief The table holding session information.
     * @details There are situations where you want to check table information and register / 
     * delete entries in the table exclusively. When using exclusive lock, contention between 
     * readers is useless. When the reader writer lock is used, the cache is frequently 
     * polluted by increasing or decreasing the reference count. Therefore, lock-free exclusive 
     * arbitration is performed for fixed-length tables.
     * @attention Please set KVS_MAX_PARALLEL_THREADS larger than actual number of sessions.
     */
    static inline std::array<session, KVS_MAX_PARALLEL_THREADS> session_table_;
};
} // namespace shirakami