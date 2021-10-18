/**
 * @file session.h
 */

#pragma once

#include <array>
#include <atomic>

#include "cpu.h"
#include "epoch.h"
#include "local_set.h"

#include "concurrency_control/wp/include/tid.h"

#include "yakushima/include/kvs.h"

namespace shirakami {

enum class tx_mode : char {
    BATCH,
    OCC,
};

class alignas(CACHE_LINE_SIZE) session {
public:
    using node_set_type = std::vector<std::pair<yakushima::node_version64_body,
                                                yakushima::node_version64*>>;

    /**
     * @brief compare and swap for visible_.
     */
    bool cas_visible(bool& expected, bool& desired) {
        return visible_.compare_exchange_weak(expected, desired,
                                              std::memory_order_release,
                                              std::memory_order_acquire);
    }

    /**
     * @brief check the existance of @a storage in wp_set_.
     * @return true exist.
     * @return false not exist.
     */
    bool check_exist_wp_set(Storage storage);

    /**
     * @brief clean up about local set.
     */
    void clean_up_local_set();

    /**
     * @brief clean up tx_began.
     */
    void clean_up_tx_property();

    /**
     * @brief get the value of mrc_tid_.
     */
    [[nodiscard]] tid_word get_mrc_tid() const { return mrc_tid_; }

    /**
     * @brief getter of @a read_only_
     */
    [[nodiscard]] bool get_read_only() const { return read_only_; }

    /**
     * @brief get the value of tx_began_.
     */
    [[nodiscard]] bool get_tx_began() { return tx_began_.load(std::memory_order_acquire); }

    /**
     * @brief getter of @a mode_.
     */
    [[nodsicard]] tx_mode get_mode() const { return mode_; }

    /**
     * @brief getter of @a valid_epoch_
     */
    [[nodiscard]] epoch::epoch_t get_valid_epoch() const { return valid_epoch_; }

    /**
     * @brief get the value of visible_.
     */
    [[nodsicard]] bool get_visible() { return visible_.load(std::memory_order_acquire); }

    std::vector<Storage>& get_wp_set() { return wp_set_; }

    /**
     * @brief get the local write set.
     */
    local_write_set& get_write_set() { return write_set_; }

    /**
     * @brief get the yakushima token used by this session.
     */
    yakushima::Token get_yakushima_token() { return yakushima_token_; }

    void push_to_read_set(read_set_obj&& elem) {
        read_set_.emplace_back(std::move(elem));
    }

    void push_to_write_set(write_set_obj&& elem) {
        write_set_.push(std::move(elem));
    }

    void set_mrc_tid(tid_word const& tidw) { mrc_tid_ = tidw; }

    void set_read_only(bool tf) { read_only_ = tf; }

    void set_tx_began(bool tf) {
        tx_began_.store(tf, std::memory_order_release);
    }

    void set_mode(tx_mode mode) { mode_ = mode; }

    void set_valid_epoch(epoch::epoch_t ep) { valid_epoch_ = ep; }

    void set_visible(bool tf) { visible_.store(tf, std::memory_order_release); }

    void set_wp_set(std::vector<Storage> const& wps) { wp_set_ = wps; }

    void set_yakushima_token(yakushima::Token token) {
        yakushima_token_ = token;
    }

private:
    tx_mode mode_{tx_mode::OCC};

    /**
     * @brief If this is true, begun transaction by this session can only do (transaction read operations).
     */
    bool read_only_{false};

    /**
     * @brief most recently chosen tid for calculate new tid of occ.
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
     * @brief local wp set.
     * @details If this session processes long transaction in a batch mode and 
     * executes transactional write operations, it is for cheking whether the target 
     * of the operation was write preserved properly by use this infomation.
     */
    std::vector<Storage> wp_set_{};

    /**
     * @brief local write set.
     */
    local_write_set write_set_{};

    /**
     * @brief token about yakushima.
     */
    yakushima::Token yakushima_token_{};

    /**
     * @brief local set for phantom avoidance.
     */
    node_set_type node_set_{};

    // for batch field
    /**
     * @brief read write batch executes write preserve preserve.
     */
    epoch::epoch_t valid_epoch_{};
};

class session_table {
public:
    /**
     * @brief Acquire right of an one session.
     */
    static Status decide_token(Token& token); // NOLINT

    /**
     * @brief End work about session_table.
     */
    static void fin_session_table();

    /**
     * @brief getter of session_table_
     */
    static std::array<session, KVS_MAX_PARALLEL_THREADS>& get_session_table() {
        return session_table_;
    }

    /**
     * @brief Initialization about session_table_
     */
    static void init_session_table(bool enable_recovery);

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
    static inline std::array<session, KVS_MAX_PARALLEL_THREADS> // NOLINT
            session_table_;                                     // NOLINT
};
} // namespace shirakami