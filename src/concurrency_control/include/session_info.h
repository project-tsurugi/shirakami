/**
 * @file session_info.h
 * @brief private scheme of transaction engine
 */

#pragma once

#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "compiler.h"
#include "cpu.h"
#include "fileio.h"
#include "record.h"
#include "local_set.h"
#include "tid.h"

#include "concurrency_control/include/cleanup_manager.h"
#include "concurrency_control/include/garbage_manager.h"

// shirakami/include/
#include "shirakami/scheme.h"

#include "yakushima/include/kvs.h"

#if defined(PWAL)

#include "fault_tolerance/include/pwal.h"

#elif defined(CPR)

#include "fault_tolerance/include/cpr.h"

#include <tsl/hopscotch_map.h>

using namespace shirakami::cpr;

#endif

namespace shirakami {

class session_info {
public:
    class scan_handler {
    public:
        using scan_cache_type = std::map<ScanHandle, std::tuple<Storage, std::vector<std::tuple<const Record*, yakushima::node_version64_body, yakushima::node_version64*>>>>;
        using scan_cache_itr_type = std::map<ScanHandle, std::size_t>;
        static constexpr std::size_t scan_cache_storage_pos = 0;
        static constexpr std::size_t scan_cache_vec_pos = 1;

        [[maybe_unused]] scan_cache_type& get_scan_cache() { // NOLINT
            return scan_cache_;
        }

        [[maybe_unused]] scan_cache_itr_type& get_scan_cache_itr() { // NOLINT
            return scan_cache_itr_;
        }

    private:
        scan_cache_type scan_cache_{};
        scan_cache_itr_type scan_cache_itr_{};
    };

    explicit session_info(Token token) {
        this->token_ = token;
        get_mrctid().reset();
#if defined(PWAL)
        get_flushed_ctid().reset();
#endif
    }

    session_info() {
        this->visible_.store(false, std::memory_order_release);
        get_mrctid().reset();
#if defined(PWAL)
        get_flushed_ctid().reset();
#endif
    }

    /**
     * @brief clean up about holding operation info.
     */
    void clean_up_ops_set();

    /**
     * @brief clean up about scan operation.
     */
    void clean_up_scan_caches();

    /**
     * @brief for debug.
     */
    [[maybe_unused]] void display_read_set();

    [[maybe_unused]] void display_write_set();

    bool cas_visible(bool& expected, bool& desired) { // NOLINT
        return visible_.compare_exchange_strong(expected, desired,
                                                std::memory_order_acq_rel);
    }

    /**
     * @brief check whether it already executed update or insert operation.
     * @param[in] storage
     * @param[in] key the key of record.
     * @pre this function is only executed in delete_record operation.
     * @return Status::OK no update/insert before this delete_record operation.
     * @return Status::WARN_CANCEL_PREVIOUS_OPERATION it canceled an update/insert
     * operation before this delete_record operation.
     */
    Status check_delete_after_write(Storage storage, std::string_view key); // NOLINT

    [[nodiscard]] epoch::epoch_t get_epoch() const { // NOLINT
        return epoch_.load(std::memory_order_acquire);
    }

    tid_word& get_mrctid() { return mrc_tid_; } // NOLINT

    std::vector<read_set_obj>& get_read_set() { // NOLINT
        return read_set;
    }

    bool get_read_only() const { // NOLINT
        return read_only_;
    }

    std::map<ScanHandle, std::size_t>& get_scan_cache_itr() { // NOLINT
        return scan_handle_.get_scan_cache_itr();
    }

    [[maybe_unused]] Token& get_token() { return token_; } // NOLINT

    [[maybe_unused]] [[nodiscard]] const Token& get_token() const { // NOLINT
        return token_;
    }

    bool get_txbegan() const { return tx_began_.load(std::memory_order_acquire); } // NOLINT

    [[nodiscard]] bool get_visible() const { // NOLINT
        return visible_.load(std::memory_order_acquire);
    }

    std::vector<write_set_obj>& get_write_set() { // NOLINT
        return write_set;
    }

    std::vector<Tuple>& get_read_only_tuples() { // NOLINT
        return read_only_tuples_;
    }

    /**
     * @brief Remove inserted records of write set from masstree.
     *
     * Insert operation inserts records to masstree in read phase.
     * If the transaction is aborted, the records exists for ever with absent
     * state. So it needs to remove the inserted records of write set from
     * masstree at abort.
     * @pre This function is called at abort.
     */
    void remove_inserted_records_of_write_set_from_masstree();

    /**
     * @brief check whether it already executed write operation.
     * @param[in] rec_ptr the target record.
     * @return the pointer of element. If it is nullptr, it is not found.
     */
    write_set_obj* search_write_set(const Record* const rec_ptr); // NOLINT

    /**
     * @brief unlock records in write set.
     *
     * This function unlocked all records in write set absolutely.
     * So it has a pre-condition.
     * @pre It has locked all records in write set.
     * @return void
     */
    void unlock_write_set();

    /**
     * @brief unlock write set object between @a begin and @a end.
     * @param [in] begin Starting points.
     * @param [in] end Ending points.
     * @pre It already locked write set between @a begin and @a end.
     * @return void
     */
    void unlock_write_set(std::vector<write_set_obj>::iterator begin,
                          std::vector<write_set_obj>::iterator end);

    /**
     * begin setter zone
     */
    /**
     * @param epoch
     */
    void set_epoch(epoch::epoch_t epoch) {
        epoch_.store(epoch, std::memory_order_release);
    }

    void set_mrc_tid(const tid_word& tid) { mrc_tid_ = tid; }

    void set_read_only(const bool tf) { read_only_ = tf; }

    [[maybe_unused]] void set_token(Token token) { token_ = token; }

    void set_tx_began(bool tf) { tx_began_.store(tf, std::memory_order_release); }

    void set_visible(bool visible) {
        visible_.store(visible, std::memory_order_release);
    }
    /**
     * end setter zone
     */

#ifdef PWAL

    pwal::pwal_handler& get_log_handler() { // NOLINT
        return log_handle_;
    }

    std::vector<pwal::LogRecord>& get_log_set() { // NOLINT
        return log_handle_.get_log_set();
    }

    tid_word& get_flushed_ctid() { return log_handle_.get_flushed_ctid(); } // NOLINT

    /**
     * @brief write-ahead logging
     * @param [in] commit_id commit tid.
     * @return void
     */
    void pwal(std::uint64_t commit_id, commit_property cp);

    void set_flushed_ctid(const tid_word& ctid) {
        log_handle_.set_flushed_ctid(ctid);
    }
#endif

    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>>& get_node_set() { // NOLINT
        return node_set;
    }

    scan_handler::scan_cache_type& get_scan_cache() { // NOLINT
        return scan_handle_.get_scan_cache();
    }

    [[nodiscard]] yakushima::Token get_yakushima_token() { // NOLINT
        return yakushima_token_;
    }

    void set_kvs_token(yakushima::Token new_token) {
        yakushima_token_ = new_token;
    }

    /**
     * @pre This function is called by insert functions.
     * @brief It is a function for preventing from phantom problems which is occurred by itself.
     * @param nvp
     * @return Status::OK success.
     * @return Status::ERR_PHANTOM It fails because its insert operation occur a phantom problem by itself.
     */
    Status update_node_set(yakushima::node_version64* nvp); // NOLINT

#if defined(CPR)

    void clear_diff_set() { return cpr_local_handle_.clear_diff_set(); }

    /**
     * @pre In this function, the worker thread selects the appropriate container from the mechanism 
     * that switches the container that stores information from time to time. Do not call from CPR manager.
     * CPR managers have different criteria for choosing containers.
     */
    cpr_local_handler::diff_upd_set_type& get_diff_upd_set() { return cpr_local_handle_.get_diff_upd_set(); }

    cpr_local_handler::diff_upd_seq_set_type& get_diff_upd_seq_set() { return cpr_local_handle_.get_diff_upd_seq_set(); }

    cpr_local_handler::diff_upd_set_type& get_diff_upd_set(std::size_t index) { return cpr_local_handle_.get_diff_upd_set(index); }

    cpr_local_handler::diff_upd_seq_set_type& get_diff_upd_seq_set(std::size_t index) { return cpr_local_handle_.get_diff_upd_seq_set(index); }

    cpr::phase get_phase() { return cpr_local_handle_.get_phase(); }

    std::uint64_t get_version() { return cpr_local_handle_.get_version(); }

    void regi_diff_upd_set(std::string_view storage, tid_word tid, Record* record, OP_TYPE op_type);

    void regi_diff_upd_seq_set(SequenceValue id, std::tuple<SequenceVersion, SequenceValue> ver_val);

    void reserve_diff_set() { return cpr_local_handle_.reserve_diff_set(); }

    void update_pv() {
        cpr_local_handle_.set_phase_version(cpr::global_phase_version::get_gpv());
    }

#endif

    // about cleanup handle
    cleanup_manager::cleanup_handler& get_cleanup_handle() {
        return cleanup_handle_;
    }

    // about gc handle
    garbage_manager::gc_handler& get_gc_handle() {
        return gc_handle_;
    }

private:
    alignas(CACHE_LINE_SIZE) Token token_{};
    tid_word mrc_tid_{}; // most recently chosen tid, for calculate new tids.
    std::atomic<epoch::epoch_t> epoch_{0};
    /**
     * @brief If this is true, this session is live, otherwise, not live.
     */
    std::atomic<bool> visible_{false};
    /**
     * @brief If this is true, this session is in some tx, otherwise, not.
     */
    std::atomic<bool> tx_began_{false};
    /**
     * @brief If this is true, begun transaction by this session can only do (transaction read operations).
     */
    bool read_only_{false};
    std::vector<Tuple> read_only_tuples_{};

    /**
     * about holding operation info.
     */
    std::vector<read_set_obj> read_set{};
    std::vector<write_set_obj> write_set{};

    /**
     * about scan operation.
     */
    scan_handler scan_handle_;

    /**
     * about snapshot
     */
    cleanup_manager::cleanup_handler cleanup_handle_;

    /**
     * about gc
     */
    garbage_manager::gc_handler gc_handle_;

    /**
     * about indexing.
     */
    yakushima::Token yakushima_token_{};
    std::vector<std::pair<yakushima::node_version64_body, yakushima::node_version64*>> node_set{};

    /**
     * about logging.
     */
#if defined(PWAL)
    pwal::pwal_handler log_handle_;
#elif defined(CPR)
    cpr_local_handler cpr_local_handle_;
#endif
};

} // namespace shirakami
