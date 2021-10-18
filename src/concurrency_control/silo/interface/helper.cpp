/**
 * @file helper.cpp
 */

#include <glog/logging.h>

#include "include/helper.h"

#include "concurrency_control/silo/include/cleanup_manager.h"
#include "concurrency_control/silo/include/garbage_manager.h"
#include "concurrency_control/silo/include/session_table.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#if defined(PWAL) || defined(CPR)

#include "fault_tolerance/include/log.h"

#endif

#include "shirakami/interface.h"

namespace shirakami {

Status enter(Token& token) { // NOLINT
    Status ret_status = session_table::decide_token(token);
    if (ret_status != Status::OK) return ret_status;
    yakushima::Token kvs_token{};
    while (yakushima::enter(kvs_token) != yakushima::status::OK) {
        _mm_pause();
    }
    static_cast<session*>(token)->set_yakushima_token(kvs_token);
    return ret_status;
}

void fin([[maybe_unused]] bool force_shut_down_cpr) try {
    if (!get_initialized()) { return; }

    /** It may seem like a meaningless order at first glance. Actually, 
     * it is an order to make the end processing asynchronous as much as possible. 
     * Send an end signal from a costly thread. Synchronize when needed.
     * Delay synchronization as much as possible.
     */

    cleanup_manager::set_cleanup_manager_thread_end(true);
    snapshot_manager::set_snapshot_manager_thread_end(true);
    garbage_manager::set_garbage_manager_thread_end(true);
#ifdef CPR
    cpr::set_checkpoint_thread_end_force(force_shut_down_cpr);
    cpr::set_checkpoint_thread_end(true);
#endif
    epoch::set_epoch_thread_end(true);

    cleanup_manager::
            join_cleanup_manager_thread(); // Cond : before delete table, before joining garbage_manager.
    snapshot_manager::
            join_snapshot_manager_thread(); // Cond : before delete table because this access current records.
#ifdef CPR
    cpr::join_checkpoint_thread();
    // Cond : before delete table because this access current records.
#endif

    delete_all_records();
    garbage_manager::
            join_garbage_manager_thread(); // Cond : before release_all_heap_objects func.
    garbage_manager::release_all_heap_objects();


    // Clean up
    session_table::fin_session_table();
    yakushima::fin();           // Cond : after all removing (from index) ops.
    epoch::join_epoch_thread(); // Note : this can be called at last.
    set_initialized(false);
} catch (std::exception& e) {
    std::cerr << "fin() : " << e.what() << std::endl;
}

Status
init([[maybe_unused]] bool enable_recovery,
     [[maybe_unused]] const std::string_view log_directory_path) { // NOLINT

    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }
// start about logging
#if defined(PWAL) || defined(CPR)
    /**
     * The default value of log_directory is PROJECT_ROOT.
     */
    Log::set_kLogDirectory(log_directory_path);
    if (log_directory_path == MAC2STR(PROJECT_ROOT)) {
        std::string log_dir = Log::get_kLogDirectory();
        log_dir.append("/log");
        Log::set_kLogDirectory(log_dir);
    }

    /**
     * check whether log_directory_path is filesystem objects.
     */
    boost::filesystem::path log_dir{Log::get_kLogDirectory()};
    if (boost::filesystem::exists(log_dir)) {
        /**
         * some file exists.
         * check whether it is directory.
         */
        if (!boost::filesystem::is_directory(log_dir)) {
            return Status::WARN_INVALID_ARGS;
        }

        if (enable_recovery) {
            /**
             * If it already exists log files, it recoveries from those.
             */
            Log::recovery_from_log();
        }
        boost::filesystem::remove_all(log_dir);
    }
    boost::filesystem::create_directories(log_dir);

#endif
    // end about logging

    session_table::init_session_table(enable_recovery);
    epoch::invoke_epocher();
    snapshot_manager::invoke_snapshot_manager();
    cleanup_manager::invoke_cleanup_manager();
    garbage_manager::invoke_garbage_manager();

    yakushima::init();

#ifdef CPR
    cpr::invoke_checkpoint_thread();
#endif

    set_initialized(true);
    return Status::OK;
}

Status leave(Token const token) { // NOLINT
    for (auto&& itr : session_table::get_session_table()) {
        if (&itr == static_cast<session*>(token)) {
            if (itr.get_visible()) {
                yakushima::leave(
                        static_cast<session*>(token)->get_yakushima_token());
                itr.set_tx_began(false);
                itr.set_visible(false);
                return Status::OK;
            }
            return Status::WARN_NOT_IN_A_SESSION;
        }
    }
    return Status::WARN_INVALID_ARGS;
}

Status
tx_begin(Token const token, bool const read_only, bool const for_batch,
         [[maybe_unused]] std::vector<Storage> write_preserve) { // NOLINT
    auto* ti = static_cast<session*>(token);
    if (ti->get_txbegan()) { return Status::WARN_ALREADY_BEGIN; }

    /**
      * This func includes loading latest global epoch used for epoch-base resource management. This means that this
      * func is also bound of epoch-base resource management such as view management for gc to prevent segv. So this
      * func is called once by each tx.
      */
    ti->set_tx_began(true);
    ti->set_epoch(epoch::get_global_epoch());
    ti->set_read_only(read_only);
    ti->get_write_set().set_for_batch(for_batch);
#if defined(CPR)
    ti->update_pv();
#endif

    return Status::OK;
}

Status read_record(Record& res, const Record* const dest) { // NOLINT
    tid_word f_check;
    tid_word s_check; // first_check, second_check for occ

    f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));

    for (;;) {
        auto return_some_others_write_status = [&f_check] {
            if (f_check.get_absent() && f_check.get_latest()) {
                return Status::WARN_CONCURRENT_INSERT;
            }
            if (f_check.get_absent() && !f_check.get_latest()) {
                return Status::WARN_CONCURRENT_DELETE;
            }
            return Status::WARN_CONCURRENT_UPDATE;
        };

#if PARAM_RETRY_READ > 0
        auto check_concurrent_others_write = [&f_check] {
            if (f_check.get_absent()) {
                if (f_check.get_latest()) {
                    return Status::WARN_CONCURRENT_INSERT;
                    // other thread is inserting this record concurrently,
                    // but it isn't committed yet.
                }
                return Status::WARN_CONCURRENT_DELETE;
            }
            return Status::OK;
        };

        std::size_t repeat_num{0};
#endif
        while (f_check.get_lock()) {
#if PARAM_RETRY_READ == 0
            return return_some_others_write_status();
#else
            if (repeat_num >= PARAM_RETRY_READ) {
                return return_some_others_write_status();
            }
            _mm_pause();
            f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
            Status s{check_concurrent_others_write()};
            if (s != Status::OK) return s;
            ++repeat_num;
#endif
        }

        if (f_check.get_absent()) {
            /**
             * Detected records that were deleted but remained in the index so that CPR threads could be found, or read
             * only snapshot transactions could be found.
             */
            return Status::WARN_CONCURRENT_DELETE;
        }

        res.get_tuple().get_pimpl()->set(
                dest->get_tuple().get_key(),
                dest->get_tuple().get_pimpl_cst()->get_val_ptr());
        // todo optimization by shallow copy about key (Now, key is deep copy, value is shallow copy).

        s_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
        if (f_check == s_check) { break; }
        f_check = s_check;
    }

    res.set_tidw(f_check);
    return Status::OK;
}

void write_phase(session* const ti, const tid_word& max_r_set,
                 const tid_word& max_w_set,
                 [[maybe_unused]] commit_property cp) {
    /*
     * It calculates the smallest number that is
     * (a) larger than the TID of any record read or written by the transaction,
     * (b) larger than the worker's most recently chosen TID,
     * and (C) in the current global epoch.
     */
    tid_word tid_a;
    tid_word tid_b;
    tid_word tid_c;

    /*
     * calculates (a)
     * about read_set
     */
    tid_a = std::max(max_w_set, max_r_set);
    tid_a.inc_tid();

    /*
     * calculates (b)
     * larger than the worker's most recently chosen TID,
     */
    tid_b = ti->get_mrctid();
    tid_b.inc_tid();

    /* calculates (c) */
    tid_c.set_epoch(ti->get_epoch());

    /* compare a, b, c */
    tid_word max_tid = std::max({tid_a, tid_b, tid_c});
    max_tid.set_lock(false);
    max_tid.set_absent(false);
    max_tid.set_latest(true);
    max_tid.set_epoch(ti->get_epoch());
    ti->set_mrc_tid(max_tid);

#ifdef PWAL
    ti->pwal(max_tid.get_obj(), cp);
#endif

    auto process = [ti, max_tid](write_set_obj* we_ptr) {
        Record* rec_ptr = we_ptr->get_rec_ptr();
#ifdef CPR
        std::string_view value_view{};
        if (we_ptr->get_op() == OP_TYPE::INSERT) {
            value_view = rec_ptr->get_tuple().get_value();
        } else if (we_ptr->get_op() == OP_TYPE::UPDATE) {
            value_view = we_ptr->get_tuple().get_value();
        }
        ti->regi_diff_upd_set(we_ptr->get_storage(), max_tid, we_ptr->get_op(),
                              rec_ptr, value_view);
#endif
        auto safely_snap_work = [&rec_ptr, &ti] {
            if (snapshot_manager::get_snap_epoch(ti->get_epoch()) !=
                snapshot_manager::get_snap_epoch(
                        rec_ptr->get_tidw().get_epoch())) {
                // update safely snap
                auto* new_rec = // NOLINT
                        new Record(rec_ptr->get_tuple().get_key(),
                                   rec_ptr->get_tuple().get_value());
                new_rec->get_tidw().set_epoch(rec_ptr->get_tidw().get_epoch());
                new_rec->get_tidw().set_latest(true);
                new_rec->get_tidw().set_lock(false);
                new_rec->get_tidw().set_absent(false);
                if (rec_ptr->get_snap_ptr() == nullptr) {
                    // create safely snap
                    rec_ptr->set_snap_ptr(new_rec);
                } else {
                    // create safely snap and insert at second.
                    new_rec->set_snap_ptr(rec_ptr->get_snap_ptr());
                    rec_ptr->set_snap_ptr(new_rec);
                }
                ti->get_gc_handle().get_snap_cont().push(
                        std::make_pair(ti->get_epoch(), new_rec));
            }
        };
        switch (we_ptr->get_op()) {
            case OP_TYPE::INSERT: {
#ifdef CPR
                /**
                 * When this transaction started in the rest phase, cpr thread never started scanning.
                 */
                if (ti->get_phase() == cpr::phase::REST) {
                    /**
                     * If this transaction have a serialization point after the checkpoint boundary and before the scan
                     * of the cpr thread, the cpr thread will include it even though it should not be included in the
                     * checkpoint.
                     * Since this transaction inserted this record with a lock, it is guaranteed that the checkpoint
                     * thread has never been reached.
                     */
                    rec_ptr->set_version(ti->get_version());
                } else {
                    rec_ptr->set_version(ti->get_version() + 1);
                }
#endif
                storeRelease(rec_ptr->get_tidw().get_obj(), max_tid.get_obj());
                break;
            }
            case OP_TYPE::UPDATE: {
                safely_snap_work();
#ifdef CPR
                // update about cpr
                if (ti->get_phase() != cpr::phase::REST &&
                    rec_ptr->get_version() != (ti->get_version() + 1)) {
                    rec_ptr->get_stable() = rec_ptr->get_tuple();
                    rec_ptr->set_version(ti->get_version() + 1);
                }
#endif
                // update value
                std::string* old_value{};
                std::string_view new_value_view =
                        we_ptr->get_tuple(we_ptr->get_op()).get_value();
                rec_ptr->get_tuple().get_pimpl()->set_value(
                        new_value_view.data(), new_value_view.size(),
                        &old_value);
                if (old_value != nullptr) {
                    ti->get_gc_handle().get_val_cont().push(
                            std::make_pair(old_value, ti->get_epoch()));
                } else {
                    /**
                     *  null insert is not expected.
                     */
                    LOG(FATAL) << "Null insert is not expected.";
                }
                storeRelease(rec_ptr->get_tidw().get_obj(), max_tid.get_obj());
                break;
            }
            case OP_TYPE::DELETE: {
                safely_snap_work();
                tid_word delete_tid = max_tid;
                delete_tid.set_latest(false);
                delete_tid.set_absent(true);

#ifdef CPR
                if (ti->get_phase() != cpr::phase::REST) {
                    /**
                     * This is in checkpointing phase (in-progress or wait-flush), meaning checkpoint thread may be scanning.
                     */
                    if (ti->get_version() + 1 != rec_ptr->get_version()) {
                        rec_ptr->get_stable() = rec_ptr->get_tuple();
                        rec_ptr->set_version(ti->get_version() + 1);
                    }
                } else {
                    /**
                     * Used by the manager as information about when memory can be freed.
                     */
                    rec_ptr->set_version(ti->get_version());
                }
#endif
                storeRelease(rec_ptr->get_tidw().get_obj(),
                             delete_tid.get_obj());
                ti->get_cleanup_handle().push(
                        {std::string(we_ptr->get_storage()), rec_ptr});
                break;
            }
            default:
                LOG(FATAL) << "unknown operation type.";
        }
    };

    if (ti->get_write_set().get_for_batch()) {
        for (auto&& elem : ti->get_write_set().get_cont_for_bt()) {
            process(&std::get<1>(elem));
        }
    } else {
        for (auto&& elem : ti->get_write_set().get_cont_for_ol()) {
            process(&elem);
        }
    }
}

} // namespace shirakami
