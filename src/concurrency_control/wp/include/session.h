/**
 * @file session.h
 */

#pragma once

#include <array>
#include <atomic>

#include "cpu.h"
#include "epoch.h"
#include "garbage.h"
#include "local_set.h"

#include "concurrency_control/wp/include/tid.h"

#include "shirakami/tuple.h"

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
    using read_set_type = std::vector<read_set_obj>;

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
    [[nodiscard]] bool check_exist_wp_set(Storage storage) const;

    void clean_up() {
        clean_up_local_set();
        clean_up_tx_property();
    }

    /**
     * @brief clean up about local set.
     */
    void clean_up_local_set();

    /**
     * @brief clean up tx_began.
     */
    void clean_up_tx_property();

    /**
     * @brief Find wp about @a st from wp set.
     * @param st target storage.
     * @return Status::OK success.
     * @return Status::WARN_NOT_FOUND fail.
     */
    [[nodiscard]] Status find_wp(Storage st) const;

    [[nodiscard]] std::size_t get_batch_id() const { return batch_id_; }

    Tuple* get_cache_for_search_ptr() { return &cache_for_search_; }

    node_set_type& get_node_set() { return node_set_; }

    /**
     * @brief get the value of mrc_tid_.
     */
    [[nodiscard]] tid_word get_mrc_tid() const { return mrc_tid_; }

    /**
     * @brief getter of @a read_only_
     */
    [[nodiscard]] bool get_read_only() const { return read_only_; }

    read_set_type& get_read_set() { return read_set_; }

    std::vector<Storage>& get_storage_set() { return storage_set_; }

    /**
     * @brief get the value of tx_began_.
     */
    [[nodiscard]] bool get_tx_began() {
        return tx_began_.load(std::memory_order_acquire);
    }

    /**
     * @brief getter of @a mode_.
     */
    [[nodiscard]] tx_mode get_mode() const { return mode_; }

    [[nodiscard]] epoch::epoch_t get_step_epoch() const {
        return step_epoch_.load(std::memory_order_release);
    }

    /**
     * @brief getter of @a valid_epoch_
     */
    [[nodiscard]] epoch::epoch_t get_valid_epoch() const {
        return valid_epoch_.load(std::memory_order_acquire);
    }

    /**
     * @brief get the value of visible_.
     */
    bool get_visible() { return visible_.load(std::memory_order_acquire); }

    std::vector<Storage>& get_wp_set() { return wp_set_; }

    [[nodiscard]] const std::vector<Storage>& get_wp_set() const {
        return wp_set_;
    }

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

    void set_batch_id(std::size_t bid) { batch_id_ = bid; }

    void set_cache_for_search(Tuple tuple) {
        cache_for_search_ = std::move(tuple);
    } // NOLINT
    // because Tuple is small size data.

    void set_mrc_tid(tid_word const& tidw) { mrc_tid_ = tidw; }

    void set_read_only(bool tf) { read_only_ = tf; }

    void set_tx_began(bool tf) {
        tx_began_.store(tf, std::memory_order_release);
    }

    void set_mode(tx_mode mode) { mode_ = mode; }

    void set_step_epoch(epoch::epoch_t e) {
        step_epoch_.store(e, std::memory_order_release);
    }

    void set_valid_epoch(epoch::epoch_t ep) {
        valid_epoch_.store(ep, std::memory_order_release);
    }

    void set_visible(bool tf) { visible_.store(tf, std::memory_order_release); }

    void set_wp_set(std::vector<Storage> const& wps) { wp_set_ = wps; }

    void set_yakushima_token(yakushima::Token token) {
        yakushima_token_ = token;
    }

    Status update_node_set(yakushima::node_version64* nvp) { // NOLINT
        for (auto&& elem : node_set_) {
            if (std::get<1>(elem) == nvp) {
                yakushima::node_version64_body nvb = nvp->get_stable_version();
                if (std::get<0>(elem).get_vinsert_delete() + 1 !=
                    nvb.get_vinsert_delete()) {
                    return Status::ERR_PHANTOM;
                }
                std::get<0>(elem) = nvb; // update
                /**
                  * note : discussion.
                  * Currently, node sets can have duplicate elements. If you allow duplicates, scanning will be easier.
                  * Because scan doesn't have to do a match search, just add it to the end of node set. insert gets hard.
                  * Even if you find a match, you have to search for everything because there may be other matches.
                  * If you do not allow duplication, the situation is the opposite.
                  */
            }
        }
        return Status::OK;
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
    read_set_type read_set_{};

    /**
     * @brief cache for search api.
     * @details The search function returns Tuple *. For speeding up, the 
     * entity does not currently exist on the table, so we have it here.
     */
    Tuple cache_for_search_;

    /**
     * @brief local wp set.
     * @details If this session processes long transaction in a batch mode and 
     * executes transactional write operations, it is for cheking whether the 
     * target of the operation was write preserved properly by use this infomation.
     */
    std::vector<Storage> wp_set_{};

    /**
     * @brief local write set.
     */
    local_write_set write_set_{};

    /**
     * @brief storage set
     * @details This is for wp verify in occ mode. 
     * Record the storage that has been read / write accessed. 
     * Verification using this information is performed at the validation phase.
     */
    std::vector<Storage> storage_set_{};

    /**
     * @brief epoch at latest transactional step.
     * @details Memorize the epoch in which the latest transitional step was
     * performed. Examining this information for all workers determines some 
     * free memory space.
     */
    std::atomic<epoch::epoch_t> step_epoch_{epoch::initial_epoch};

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
    std::atomic<epoch::epoch_t> valid_epoch_{epoch::initial_epoch};

    std::size_t batch_id_{};
};

class session_table {
public:
    /**
     * @brief about gc
     * @details Clean up the garbage that each session has.
     */
    static void clean_up();

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