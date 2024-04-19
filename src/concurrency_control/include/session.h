/**
 * @file concurrency_control/include/session.h
 */

#pragma once

#include <array>
#include <atomic>
#include <mutex>
#include <set>

#include "cpu.h"
#include "epoch.h"
#include "garbage.h"
#include "local_set.h"

#ifdef PWAL

#include "lpwal.h"

#endif

#include "concurrency_control/include/local_set.h"
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
    using range_read_by_short_set_type = std::set<range_read_by_short*>;
    using read_set_for_stx_type = std::vector<read_set_obj>;
    using wp_set_type = std::vector<std::pair<Storage, wp::wp_meta*>>;
    /**
     * map <key, value>: key is table info. value is tuple information: 
     * overtaken ltxs, read information compressed to range information, 
     * 1: left key, 2: left point info, 3: right key, 4: right point info, 5:
     * whether it is initialize.
     */
    using overtaken_ltx_set_type =
            std::map<wp::wp_meta*,
                     std::tuple<std::set<std::size_t>,
                                std::tuple<std::string, scan_endpoint,
                                           std::string, scan_endpoint, bool>>>;
    using ltx_storage_read_set_type =
            std::map<Storage, std::tuple<std::string, scan_endpoint,
                                         std::string, scan_endpoint>>;
    static constexpr std::uint64_t initial_mrc_tid{0};


    /**
     * @brief call commit callback and clear the callback stored
    */
    void call_commit_callback(Status sc, reason_code rc,
                              durability_marker_type dm) {
        // get callback
        auto cb = get_commit_callback();

        // call callback if it can
        if (cb) { cb(sc, rc, dm); }

        // clear callback
        set_commit_callback({});
    }

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
        set_valid_epoch(0);
        set_was_considering_forwarding_at_once(false);
        set_is_forwarding(false);
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
     * @brief return copy of calculated wait_for from overtaken_ltx_set_.
     *
     * @return std::set<std::size_t> 
     */
    [[nodiscard]] std::set<std::size_t> copyof_wait_for() {
        std::shared_lock<std::shared_mutex> lk{mtx_overtaken_ltx_set_};
        return wait_for_;
    }

    /**
     * @brief return reference of calculated wait_for from overtaken_ltx_set_.
     * @pre got mtx_overtaken_ltx_set_
     *
     * @return std::set<std::size_t>&
     */
    [[nodiscard]] const std::set<std::size_t>& get_wait_for() const { return wait_for_; }
    std::set<std::size_t>& get_wait_for() { return wait_for_; }

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

    void init_flags_for_tx_begin() { get_result_info().clear(); }

    void init_flags_for_stx_begin() {
        init_flags_for_tx_begin();
        set_tx_type(transaction_options::transaction_type::SHORT);
        get_write_set().set_for_batch(false);
    }

    void init_flags_for_ltx_begin() {
        init_flags_for_tx_begin();
        /**
          * It may be called without check_commit for the ltx.
          * Clear metadata initialized at check_commit.
          */
        set_requested_commit(false);
        set_tx_type(transaction_options::transaction_type::LONG);
        get_write_set().set_for_batch(true);
    }

    void init_flags_for_rtx_begin() {
        init_flags_for_tx_begin();
        set_tx_type(transaction_options::transaction_type::READ_ONLY);
    }

    [[nodiscard]] bool is_write_only_ltx_now() {
        std::shared_lock<std::shared_mutex> lk_point_read{
                read_set_for_ltx().get_mtx_set()};
        std::shared_lock<std::shared_mutex> lk_range_read{
                get_range_read_set_for_ltx().get_mtx_set()};
        return get_tx_type() == transaction_options::transaction_type::LONG &&
               read_set_for_ltx().set().empty() &&
               get_range_read_set_for_ltx().get_set().empty();
    }

    // ========== start: getter
    [[nodiscard]] tx_id::type_session_id get_session_id() const {
        return session_id_;
    }

    [[nodiscard]] tx_id::type_higher_info get_higher_tx_counter() const {
        return higher_tx_counter_;
    }

    [[nodiscard]] tx_id::type_lower_info get_tx_counter() const {
        return tx_counter_;
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

    // ========== start: diagnostics

    [[nodiscard]] TxState::StateKind get_diag_tx_state_kind() const {
        return diag_tx_state_kind_.load(std::memory_order_acquire);
    }

    // ========== end: diagnostics

    /**
     * @brief get the value of mrc_tid_.
     */
    [[nodiscard]] tid_word get_mrc_tid() const { return mrc_tid_; }

    std::atomic<std::size_t>& get_operating() { return operating_; }

    std::mutex& get_mtx_termination() { return mtx_termination_; }

    std::shared_mutex& get_mtx_ltx_storage_read_set() {
        return mtx_ltx_storage_read_set_;
    }

    ltx_storage_read_set_type& get_ltx_storage_read_set() {
        return ltx_storage_read_set_;
    }

    range_read_set_for_ltx& get_range_read_set_for_ltx() {
        return range_read_set_for_ltx_;
    }

    std::shared_mutex& get_mtx_range_read_by_short_set() {
        return mtx_range_read_by_short_set_;
    }

    range_read_by_short_set_type& get_range_read_by_short_set() {
        return range_read_by_short_set_;
    }

    std::shared_mutex& get_mtx_read_area() { return mtx_read_area_; }

    [[nodiscard]] transaction_options::read_area get_read_area() {
        std::shared_lock<std::shared_mutex> lk{get_mtx_read_area()};
        return read_area_;
    }

    read_set_for_stx_type& get_read_set_for_stx() { return read_set_for_stx_; }

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
        return begin_epoch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] epoch::epoch_t get_step_epoch() const {
        return step_epoch_.load(std::memory_order_acquire);
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

    [[nodiscard]] bool get_was_considering_forwarding_at_once() const {
        return was_considering_forwarding_at_once_;
    }

    [[nodiscard]] bool get_is_forwarding() const { return is_forwarding_; }

    [[nodiscard]] Status get_result_requested_commit() const {
        return result_requested_commit_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t get_long_tx_id() const { return long_tx_id_; }

    overtaken_ltx_set_type& get_overtaken_ltx_set() {
        return overtaken_ltx_set_;
    }

    std::shared_mutex& get_mtx_overtaken_ltx_set() {
        return mtx_overtaken_ltx_set_;
    }

    wp_set_type& get_wp_set() { return wp_set_; }

    [[nodiscard]] const wp_set_type& get_wp_set() const { return wp_set_; }

    [[nodiscard]] epoch::epoch_t get_read_version_max_epoch() const {
        return read_version_max_epoch_.load(std::memory_order_acquire);
    }

    local_read_set_for_ltx& read_set_for_ltx() { return read_set_for_ltx_; }

    commit_callback_type get_commit_callback() { return commit_callback_; }

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
        get_operating()++;
        set_step_epoch(epoch::get_global_epoch());
    }

    void process_before_finish_step() {
        auto expected = get_operating().load(std::memory_order_acquire);
        for (;;) {
            if (expected == 0) {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "programming error.";
                break;
            }
            auto desired = expected - 1;
            if (get_operating().compare_exchange_weak(
                        expected, desired, std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                break;
            }
        }
    }

    void clear_ltx_storage_read_set() {
        std::lock_guard<std::shared_mutex> lk{get_mtx_ltx_storage_read_set()};
        get_ltx_storage_read_set().clear();
    }

    void clear_overtaken_ltx_set() {
        std::lock_guard<std::shared_mutex> lk{get_mtx_overtaken_ltx_set()};
        overtaken_ltx_set_.clear();
        wait_for_.clear();
    }

    void clear_range_read_by_short_set() {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{
                get_mtx_range_read_by_short_set()};
        get_range_read_by_short_set().clear();
    }

    void push_to_range_read_by_short_set(range_read_by_short* rrbs) {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{
                get_mtx_range_read_by_short_set()};
        get_range_read_by_short_set().insert(rrbs);
    }

    void clear_read_set_for_stx() {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{mtx_read_set_for_stx_};
        read_set_for_stx_.clear();
    }

    void insert_to_ltx_storage_read_set(Storage const st) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_ltx_storage_read_set()};
        // find entry
        auto itr = get_ltx_storage_read_set().find(st);
        if (itr == get_ltx_storage_read_set().end()) {
            // no hit
            get_ltx_storage_read_set().insert(std::make_pair(
                    st, std::make_tuple("", scan_endpoint::EXCLUSIVE, "",
                                        scan_endpoint::EXCLUSIVE)));
        }
    }

    void insert_to_ltx_storage_read_set(Storage const st,
                                        std::string const& key) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_ltx_storage_read_set()};
        // find entry
        auto itr = get_ltx_storage_read_set().find(st);
        if (itr == get_ltx_storage_read_set().end()) {
            // no hit
            get_ltx_storage_read_set().insert(std::make_pair(
                    st, std::make_tuple(key, scan_endpoint::INCLUSIVE, key,
                                        scan_endpoint::INCLUSIVE)));
        } else {
            std::string& now_lkey = std::get<0>(itr->second);
            scan_endpoint& now_lpoint = std::get<1>(itr->second);
            std::string& now_rkey = std::get<2>(itr->second);
            scan_endpoint& now_rpoint = std::get<3>(itr->second);

            // check initialize
            if (now_lkey.empty() && now_lpoint == scan_endpoint::EXCLUSIVE &&
                now_rkey.empty() && now_rpoint == scan_endpoint::EXCLUSIVE) {
                now_lkey = key;
                now_rkey = key;
                now_lpoint = scan_endpoint::INCLUSIVE;
                now_rpoint = scan_endpoint::INCLUSIVE;
                return;
            }

            // hit, check left key
            if (key < now_lkey) {
                now_lkey = key;
                if (now_lpoint == scan_endpoint::EXCLUSIVE) {
                    now_lpoint = scan_endpoint::INCLUSIVE;
                }
            }
            if (key == now_lkey && now_lpoint == scan_endpoint::EXCLUSIVE) {
                now_lpoint = scan_endpoint::INCLUSIVE;
            }
            // check right key
            if (now_rkey < key) {
                now_rkey = key;
                if (now_rpoint == scan_endpoint::EXCLUSIVE) {
                    now_rpoint = scan_endpoint::INCLUSIVE;
                }
            }
            if (key == now_rkey && now_rpoint == scan_endpoint::EXCLUSIVE) {
                now_rpoint = scan_endpoint::INCLUSIVE;
            }
        }
    }

    void push_to_read_set_for_stx(read_set_obj&& elem) {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{mtx_read_set_for_stx_};
        read_set_for_stx_.emplace_back(std::move(elem));
    }

    void push_to_write_set(write_set_obj&& elem) {
        std::lock_guard<std::shared_mutex> lk{mtx_write_set_};
        write_set_.push(this, std::move(elem));
    }

    // ========== start: setter
    void set_session_id(tx_id::type_session_id num) { session_id_ = num; }

    void set_higher_tx_counter(tx_id::type_higher_info num) {
        higher_tx_counter_ = num;
    }

    void set_tx_counter(tx_id::type_lower_info num) { tx_counter_ = num; }

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

    // ========== start: diagnostics

    void set_diag_tx_state_kind(TxState::StateKind sk) {
        diag_tx_state_kind_.store(sk, std::memory_order_release);
    }

    // ========== end: diagnostics

    void set_mrc_tid(tid_word const& tidw) { mrc_tid_ = tidw; }

    void set_operating(std::size_t num) {
        operating_.store(num, std::memory_order_release);
    }

    void set_read_area(transaction_options::read_area const& ra) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_read_area()};
        read_area_ = ra;
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

    void set_was_considering_forwarding_at_once(bool tf) {
        was_considering_forwarding_at_once_ = tf;
    }

    void set_is_forwarding(bool const tf) { is_forwarding_ = tf; }

    void set_result_requested_commit(Status st) {
        result_requested_commit_.store(st, std::memory_order_release);
    }

    void set_long_tx_id(std::size_t bid) { long_tx_id_ = bid; }

    void set_read_version_max_epoch(epoch::epoch_t const ep) {
        read_version_max_epoch_.store(ep, std::memory_order_release);
    }

    void set_valid_epoch(epoch::epoch_t ep) {
        valid_epoch_.store(ep, std::memory_order_release);
    }

    void set_commit_callback(commit_callback_type cb) {
        commit_callback_ = std::move(cb);
    }

    // ========== end: long tx

    // ========== start: result info
    void set_result(reason_code rc);
    void set_result(reason_code rc, std::string_view str);
    // ========== end: result info

    // ========== end: setter

    // ========== start: node set
    node_set& get_node_set() { return node_set_; }

    Status update_node_set(yakushima::node_version64* nvp) { // NOLINT
        return get_node_set().update_node_set(nvp);
    }

    // ========== end: node set

private:
    /**
     * @brief tx type.
     * @attention For internal. Don't clear at tx termination. This is used for
     * lock-free coordination for multi-threads.
     */
    std::atomic<transaction_options::transaction_type> tx_type_{
            transaction_options::transaction_type::SHORT};

    /**
     * @brief session id used for computing transaction id.
     */
    tx_id::type_session_id session_id_{};

    /**
     * @brief Counter of circulated tx_counter_;
     */
    tx_id::type_higher_info higher_tx_counter_{0};

    /**
     * @brief transaction counter. Session counts executed transactions by this.
     */
    tx_id::type_lower_info tx_counter_{0};

    /**
     * @brief most recently chosen tid for calculate new tid of occ.
     */
    tid_word mrc_tid_{initial_mrc_tid};

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
     * @brief for optimization for read area
     * @details ltx mode log where they read the storage and update read area
     * at commit phase.
    */
    ltx_storage_read_set_type ltx_storage_read_set_{};

    /**
     * @brief mutex for @a ltx_storage_read_set_.
    */
    std::shared_mutex mtx_ltx_storage_read_set_{};

    range_read_set_for_ltx range_read_set_for_ltx_{};

    /**
     * mutex for range_read_by_short_set_
    */
    std::shared_mutex mtx_range_read_by_short_set_;

    range_read_by_short_set_type range_read_by_short_set_{};

    std::shared_mutex mtx_read_area_{};

    /**
     * @brief read area information used for long transaction.
     */
    transaction_options::read_area read_area_{};

    /**
     * @brief local read set for stx.
     */
    read_set_for_stx_type read_set_for_stx_{};

    /**
     * @brief mutex for local read set for stx.
    */
    std::shared_mutex mtx_read_set_for_stx_;

    /**
     * @brief local write set.
     */
    local_write_set write_set_{};

    /**
     * @brief mutex for local write set.
    */
    std::shared_mutex mtx_write_set_;

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
    node_set node_set_{};

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
    std::atomic<std::size_t> operating_{0};

    /**
     * @brief It uses for strand thread. All data access api can run concurrently
     *  and the state of termination is confused. So that use this mutex for 
     * termination and find consistency.
    */
    std::mutex mtx_termination_{};

    // ========== start: tx state

    /**
     * @brief whether acquire_tx_state_handle api is called for current tx.
     * @details If this is true, @a current_tx_status_handle_ is valid object.
     */
    bool has_current_tx_state_handle_{false};

    std::atomic<TxStateHandle> current_tx_state_handle_{};

    TxState* current_tx_state_ptr_{};
    // ========== end: tx state

    // ========== start: diagnostics

    /**
     * @brief tx_state for diagnostics.
     */
    std::atomic<TxState::StateKind> diag_tx_state_kind_{};

    // ========== end: diagnostics

    // ========== start: long tx
    /**
     * @brief Whether the long tx was already requested commit.
     * @note It may be accessed by user and shirakami background worker.
     */
    std::atomic<bool> requested_commit_{};

    /**
     * @brief whether this tx was considering forwarding at once. This is used
     * for considering read wait
    */
    bool was_considering_forwarding_at_once_{false};

    /**
     * @brief This is forwarding as ltx
     * 
    */
    bool is_forwarding_{false};

    /**
     * @brief The requested transaction commit status which is/was decided 
     * shirakami manager.
     * @note It may be accessed by user and shirakami background worker.
     */
    std::atomic<Status> result_requested_commit_{};

    /**
     * @brief Whether this tx is forced to backward due to protocol logic.
    */
    std::atomic<bool> is_force_backwarding_{false};

    /**
     * @brief long tx id.
     */
    std::size_t long_tx_id_{};

    /**
     * @brief The ltx set which this transaction overtook.
     */
    overtaken_ltx_set_type overtaken_ltx_set_;

    std::set<std::size_t> wait_for_;

    /**
     * @brief The shared mutex about overtaken_ltx_set_.
     */
    std::shared_mutex mtx_overtaken_ltx_set_;

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
    std::atomic<epoch::epoch_t> read_version_max_epoch_{};

    local_read_set_for_ltx read_set_for_ltx_;

    /**
     * @brief Memory for waiting ltx
    */
    commit_callback_type commit_callback_{};

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

    /**
     * @brief print diagnostics information.
     */
    static void print_diagnostics(std::ostream& out);

private:
    /**
      * @brief The table holding session information.
      * @details There are situations where you want to check table information 
      * and register / delete entries in the table exclusively. When using 
      * exclusive lock, contention between readers is useless. When the reader 
      * writer lock is used, the cache is frequently polluted by increasing or 
      * decreasing the reference count. Therefore, lock-free exclusive 
      * arbitration is performed for fixed-length tables.
      * @attention Please set KVS_MAX_PARALLEL_THREADS larger than actual number 
      * of sessions.
      */
    static inline std::array<session, KVS_MAX_PARALLEL_THREADS> // NOLINT
            session_table_;                                     // NOLINT
};

/**
 * @brief It checks result of the transaction requested commit.
 * @param[in] token This should be the token which was used for commit api.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::ERR_KVS Error about key value store.
 * @return Status::OK This transaction was committed.
 * @return Status::WARN_ILLEGAL_OPERATION The @a token is not long transaction
 * or didn't request commit.
 * @return Status::WARN_NOT_BEGIN This transaction was not begun.
 * @return Status::WARN_WAITING_FOR_OTHER_TX The long transaction needs wait 
 * for finishing commit by other high priority tx. You must execute check_commit 
 * to check result. If you use other api (ex. data access api), it causes 
 * undefined behavior. 
 * @note If this function returns OK or ERR_..., the transaction finished. After
 * that or calling for not ltx, the result of calling this (finished) 
 * transaction is undefined behavior.
 */
Status check_commit(Token token); // NOLINT

} // namespace shirakami