/**
 * @file concurrency_control/include/session.h
 */

#pragma once

#include <array>
#include <atomic>
#include <set>

#include "cpu.h"
#include "epoch.h"
#include "garbage.h"
#include "local_set.h"

#ifdef PWAL

#include "lpwal.h"

#endif

#include "concurrency_control/include/read_by.h"
#include "concurrency_control/include/scan.h"
#include "concurrency_control/include/tid.h"
#include "concurrency_control/include/wp.h"

#include "shirakami/result_info.h"
#include "shirakami/scheme.h"
#include "shirakami/transaction_options.h"
#include "shirakami/transaction_state.h"
#include "shirakami/tuple.h"

#include "yakushima/include/kvs.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) session {
public:
    using node_set_type = std::vector<std::pair<yakushima::node_version64_body,
                                                yakushima::node_version64*>>;
    using point_read_by_long_set_type = std::set<point_read_by_long*>;
    using range_read_by_long_set_type =
            std::set<std::tuple<range_read_by_long*, std::string, scan_endpoint,
                                std::string, scan_endpoint>>;
    using point_read_by_short_set_type = std::set<point_read_by_short*>;
    using range_read_by_short_set_type = std::set<range_read_by_short*>;
    using read_set_type = std::vector<read_set_obj>;
    using wp_set_type = std::vector<std::pair<Storage, wp::wp_meta*>>;
    /**
     * map <key, value>: key is table info. value is tuple information: 
     * overtaken ltxs, read information compressed to range information.
     */
    using overtaken_ltx_set_type =
            std::map<wp::wp_meta*,
                     std::tuple<std::set<std::size_t>,
                                std::tuple<std::string, std::string>>>;


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

    void clear_about_long_tx_metadata() {
        set_read_version_max_epoch(0);
        set_long_tx_id(0);
    }

    void clear_about_tx_state() {
        if (get_has_current_tx_state_handle()) {
            // valid
            set_has_current_tx_state_handle(false);
            set_current_tx_state_handle(undefined_handle);
            set_current_tx_state_ptr(nullptr);
        }
    }

    void clear_about_scan() { scan_handle_.clear(); }

    void clean_up() {
        clear_local_set(); // this should before (*1). use long tx id info.
        clear_about_long_tx_metadata(); // (*1)
        clear_tx_property();
        clear_about_scan();
        clear_about_tx_state();
    }

    /**
     * @brief clean up about local set.
     */
    void clear_local_set();

    /**
     * @brief clean up tx_began.
     */
    void clear_tx_property();

    /**
     * @brief commit process for sequence.
     */
    void commit_sequence(tid_word ctid);

    /**
     * @brief extract wait_for from overtaken_ltx_set_.
     * 
     * @return std::set<std::size_t> 
     */
    std::set<std::size_t> extract_wait_for();

    /**
     * @brief long tx find high priority short.
     * @pre This is called by long tx.
     * @return Status::OK success
     * @return Status::WARN_PREMATURE There is a high priority short tx.
     * @return Status::ERR_FATAL programming error.
     */
    [[nodiscard]] Status find_high_priority_short() const;

    /**
     * @brief Find wp about @a st from wp set.
     * @param st target storage.
     * @return Status::OK success.
     * @return Status::WARN_NOT_FOUND fail.
     */
    [[nodiscard]] Status find_wp(Storage st) const;

    // ========== start: getter
    [[nodiscard]] std::uint32_t get_session_id() const { return session_id_; }

    [[nodiscard]] std::uint32_t get_higher_tx_counter() const {
        return higher_tx_counter_;
    }

    [[nodiscard]] std::uint64_t get_tx_counter() const { return tx_counter_; }

    [[nodiscard]] Tuple* get_cache_for_search_ptr() {
        return &cache_for_search_;
    }

    // ========== start: tx state
    [[nodiscard]] bool get_has_current_tx_state_handle() const {
        return has_current_tx_state_handle_;
    }

    [[nodiscard]] TxStateHandle get_current_tx_state_handle() const {
        return current_tx_state_handle_.load(std::memory_order_acquire);
    }

    [[nodiscard]] TxState* get_current_tx_state_ptr() const {
        return current_tx_state_ptr_;
    }
    // ========== end: tx state

    node_set_type& get_node_set() { return node_set_; }

    /**
     * @brief get the value of mrc_tid_.
     */
    [[nodiscard]] tid_word get_mrc_tid() const { return mrc_tid_; }

    [[nodiscard]] bool get_operating() const {
        return operating_.load(std::memory_order_acquire);
    }

    point_read_by_long_set_type& get_point_read_by_long_set() {
        return point_read_by_long_set_;
    }

    range_read_by_long_set_type& get_range_read_by_long_set() {
        return range_read_by_long_set_;
    }

    point_read_by_short_set_type& get_point_read_by_short_set() {
        return point_read_by_short_set_;
    }

    range_read_by_short_set_type& get_range_read_by_short_set() {
        return range_read_by_short_set_;
    }

    [[nodiscard]] transaction_options::read_area const& get_read_area() const {
        return read_area_;
    }

    std::set<wp::page_set_meta*>& get_read_positive_list() {
        return read_positive_list_;
    }

    std::set<wp::page_set_meta*>& get_read_negative_list() {
        return read_negative_list_;
    }

    read_set_type& get_read_set() { return read_set_; }

    /**
     * @brief get the value of tx_began_.
     */
    [[nodiscard]] bool get_tx_began() {
        return tx_began_.load(std::memory_order_acquire);
    }

    /**
     * @brief getter of @a mode_.
     */
    [[nodiscard]] transaction_options::transaction_type get_tx_type() const {
        return tx_type_;
    }

    scan_handler& get_scan_handle() { return scan_handle_; }

    [[nodiscard]] epoch::epoch_t get_begin_epoch() const {
        return begin_epoch_.load(std::memory_order_release);
    }

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
    [[nodiscard]] bool get_visible() {
        return visible_.load(std::memory_order_acquire);
    }

    /**
     * @brief get the local write set.
     */
    local_write_set& get_write_set() { return write_set_; }

    /**
     * @brief get the yakushima token used by this session.
     */
    [[nodiscard]] yakushima::Token get_yakushima_token() {
        return yakushima_token_;
    }

    // ========== start: long tx

    [[nodiscard]] bool get_requested_commit() const {
        return requested_commit_.load(std::memory_order_acquire);
    }

    [[nodiscard]] Status get_result_requested_commit() const {
        return result_requested_commit_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t get_long_tx_id() const { return long_tx_id_; }

    overtaken_ltx_set_type& get_overtaken_ltx_set() {
        return overtaken_ltx_set_;
    }

    wp_set_type& get_wp_set() { return wp_set_; }

    [[nodiscard]] const wp_set_type& get_wp_set() const { return wp_set_; }

    [[nodiscard]] epoch::epoch_t get_read_version_max_epoch() const {
        return read_version_max_epoch_;
    }

    local_read_set_for_ltx& read_set_for_ltx() { return read_set_for_ltx_; }

    // ========== end: long tx

    // ========== stat: result info
    result_info& get_result_info() { return result_info_; }
    // ========== end: result info

    // ========== start: logging
#if defined(PWAL)
    lpwal::handler& get_lpwal_handle() { return lpwal_handle_; }
#endif
    // ========== end: logging

    // ========== start: sequence
    local_sequence_set& sequence_set() { return sequence_set_; }
    // ========== end: sequence

    // ========== end: getter

    void process_before_start_step() {
        set_operating(true);
        set_step_epoch(epoch::get_global_epoch());
    }

    void process_before_finish_step() {
        if (!get_operating()) {
            LOG(ERROR) << "programming error";
        } else {
            set_operating(false);
        }
    }

    void push_to_read_set(read_set_obj&& elem) {
        read_set_.emplace_back(std::move(elem));
    }

    void push_to_write_set(write_set_obj&& elem) {
        write_set_.push(std::move(elem));
    }

    // ========== start: setter
    void set_session_id(std::uint32_t num) { session_id_ = num; }

    void set_higher_tx_counter(std::uint32_t num) { higher_tx_counter_ = num; }

    void set_tx_counter(std::uint64_t num) { tx_counter_ = num; }

    void set_cache_for_search(Tuple tuple) {
        cache_for_search_ = std::move(tuple);
    } // NOLINT
    // because Tuple is small size data.

    // ========== start: tx state
    void set_current_tx_state_handle(TxStateHandle hd) {
        current_tx_state_handle_.store(hd, std::memory_order_release);
    }

    void set_has_current_tx_state_handle(bool tf) {
        has_current_tx_state_handle_ = tf;
    }

    void set_current_tx_state_ptr(TxState* ptr) { current_tx_state_ptr_ = ptr; }

    void set_tx_state_if_valid(TxState::StateKind st) const {
        if (get_has_current_tx_state_handle()) {
            if (get_current_tx_state_ptr()->state_kind() != st) {
                get_current_tx_state_ptr()->set_kind(st);
            }
        }
    }

    // ========== end: tx state

    void set_mrc_tid(tid_word const& tidw) { mrc_tid_ = tidw; }

    void set_operating(bool tf) {
        operating_.store(tf, std::memory_order_release);
    }

    void set_read_area(transaction_options::read_area const& ra) {
        read_area_ = ra;
    }

    void set_read_positive_list(std::set<wp::page_set_meta*> const& plist) {
        read_positive_list_ = plist;
    }

    void set_read_negative_list(std::set<wp::page_set_meta*> const& nlist) {
        read_negative_list_ = nlist;
    }

    void set_tx_began(bool tf) {
        tx_began_.store(tf, std::memory_order_release);
    }

    void set_tx_type(transaction_options::transaction_type const tp) {
        tx_type_ = tp;
    }

    void set_begin_epoch(epoch::epoch_t e) {
        begin_epoch_.store(e, std::memory_order_release);
    }

    void set_step_epoch(epoch::epoch_t e) {
        step_epoch_.store(e, std::memory_order_release);
    }

    void set_visible(bool tf) { visible_.store(tf, std::memory_order_release); }

    void set_wp_set(wp_set_type const& wps) { wp_set_ = wps; }

    void set_yakushima_token(yakushima::Token token) {
        yakushima_token_ = token;
    }

    // ========== start: long tx

    void set_requested_commit(bool tf) {
        if (tf) {
            set_result_requested_commit(Status::WARN_WAITING_FOR_OTHER_TX);
        }
        requested_commit_.store(tf, std::memory_order_release);
    }

    void set_result_requested_commit(Status st) {
        result_requested_commit_.store(st, std::memory_order_release);
    }

    void set_long_tx_id(std::size_t bid) { long_tx_id_ = bid; }

    void set_read_version_max_epoch(epoch::epoch_t ep) {
        read_version_max_epoch_ = ep;
    }

    void set_valid_epoch(epoch::epoch_t ep) {
        valid_epoch_.store(ep, std::memory_order_release);
    }

    // ========== end: long tx

    // ========== start: result info
    void set_result(reason_code rc);
    void set_result(reason_code rc, std::string_view str);
    // ========== end: result info

    // ========== end: setter

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
    /**
     * @brief tx type.
     * @attention For internal. Don't clear at tx termination. This is used for
     * lock-free coordination for multi-threads.
     */
    transaction_options::transaction_type tx_type_{
            transaction_options::transaction_type::SHORT};

    /**
     * @brief session id used for computing transaction id.
     */
    std::uint32_t session_id_{};

    /**
     * @brief Counter of circulated tx_counter_;
     */
    std::uint32_t higher_tx_counter_{0};

    /**
     * @brief transaction counter. Session counts executed transactions by this.
     */
    std::uint64_t tx_counter_{0};

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

    point_read_by_long_set_type point_read_by_long_set_{};

    range_read_by_long_set_type range_read_by_long_set_{};

    point_read_by_short_set_type point_read_by_short_set_{};

    range_read_by_short_set_type range_read_by_short_set_{};

    /**
     * @brief read area information used for long transaction.
     */
    transaction_options::read_area read_area_{};

    /**
     * @brief cache about computation around read_area_
     */
    std::set<wp::page_set_meta*> read_positive_list_{};

    /**
     * @brief cache about computation around read_area_
     */
    std::set<wp::page_set_meta*> read_negative_list_{};

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
     * @brief local write set.
     */
    local_write_set write_set_{};

    /**
     * @brief The begin epoch of stx transaction begin used for GC.
     */
    std::atomic<epoch::epoch_t> begin_epoch_{epoch::initial_epoch};

    /**
     * @brief The stx's step epoch used for judge whether a ltx can start.
     * @attention For internal. Don't clear at tx termination. This is used for
     * lock-free coordination for multi-threads.
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

    /**
     * @brief about scan operation.
     */
    scan_handler scan_handle_;

    /**
     * @brief This variable shows whether this session (transaction (tx)) is processing 
     * public api now.
     * @details Process from public api of tx update @a valid_epoch_.
     * But that is not update when public api of tx is not called.
     * So if public api of tx is not called for a long time, 
     * @a valid_epoch_ is also not updated. And if @a valid_epoch_ is not 
     * update for a long time, long tx can not find to be able to start 
     * process because the short tx may be serialized before the long tx.
     * To resolve that situation, it use this variable for background thread 
     * to update @a valid_epoch_  automatically.
     */
    std::atomic<bool> operating_{false};

    // ========== start: tx state

    /**
     * @brief whether acquire_tx_state_handle api is called for current tx.
     * @details If this is true, @a current_tx_status_handle_ is valid object.
     */
    bool has_current_tx_state_handle_{false};

    std::atomic<TxStateHandle> current_tx_state_handle_{};

    TxState* current_tx_state_ptr_{};
    // ========== end: tx state

    // ========== start: long tx
    /**
     * @brief Whether the long tx was already requested commit.
     * @note It may be accessed by user and shirakami background worker.
     */
    std::atomic<bool> requested_commit_{};

    /**
     * @brief The requested transaction commit status which is/was decided 
     * shirakami manager.
     * @note It may be accessed by user and shirakami background worker.
     */
    std::atomic<Status> result_requested_commit_{};

    /**
     * @brief long tx id.
     */
    std::size_t long_tx_id_{};

    /**
     * @brief The ltx set which this transaction overtook.
     */
    overtaken_ltx_set_type overtaken_ltx_set_;

    /**
     * @brief read write batch executes write preserve preserve.
     */
    std::atomic<epoch::epoch_t> valid_epoch_{epoch::initial_epoch};

    /**
     * @brief local wp set.
     * @details If this session processes long transaction in a long tx mode and 
     * executes transactional write operations, it is for checking whether the 
     * target of the operation was write preserved properly by use this 
     * infomation.
     */
    wp_set_type wp_set_{};

    /**
     * @brief The max (created) epoch in the versions which was read by this 
     * long tx.
     * @details When a transaction attempts a preamble, it checks this value 
     * to determine if it is breaking its boundaries.
     */
    epoch::epoch_t read_version_max_epoch_{};

    local_read_set_for_ltx read_set_for_ltx_;

    // ========== end: long tx

    // ========== start: result information
    result_info result_info_{};
    // ========== end: result information

    // ========== start: sequence
    local_sequence_set sequence_set_;
    // ========== end: sequence

    // ========== start: logging
#if defined(PWAL)
    /**
     * @brief The handle about pwal for datastore limestone.
     */
    lpwal::handler lpwal_handle_{};
#endif
    // ========== end: logging
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
     * @brief getter of session_table_
     */
    static std::array<session, KVS_MAX_PARALLEL_THREADS>& get_session_table() {
        return session_table_;
    }

    /**
     * @brief Initialization about session_table_
     */
    static void init_session_table();

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