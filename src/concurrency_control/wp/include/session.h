/**
 * @file session.h
 */

#pragma once

#include <atomic>

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
} // namespace shirakami