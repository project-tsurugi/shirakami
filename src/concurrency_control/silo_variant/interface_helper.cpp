/**
 * @file helper.cpp
 */

#include "logger.h"

#include "concurrency_control/silo_variant//include/interface_helper.h"

#include "concurrency_control/silo_variant/include/garbage_collection.h"
#include "concurrency_control/silo_variant/include/session_info_table.h"
#include "concurrency_control/silo_variant/include/snapshot_manager.h"

#if defined(PWAL) || defined(CPR)
#include "log.h"
#endif

#include "kvs/interface.h"

namespace shirakami::cc_silo_variant {

Status enter(Token &token) {  // NOLINT
    Status ret_status = session_info_table::decide_token(token);
    yakushima::Token kvs_token{};
    yakushima::enter(kvs_token);
    static_cast<session_info*>(token)->set_kvs_token(kvs_token);
    return ret_status;
}

void fin() {
#ifdef CPR
    // Stop Checkpointing
    cpr::set_checkpoint_thread_end(true);
    cpr::join_checkpoint_thread();
#endif

    delete_all_records();
    garbage_collection::release_all_heap_objects();

    // Stop DB operation.
    epoch::set_epoch_thread_end(true);
    epoch::join_epoch_thread();
    snapshot_manager::set_snapshot_manager_thread_end(true);
    snapshot_manager::join_snapshot_manager_thread();
    session_info_table::fin_kThreadTable();

    yakushima::fin();
}

Status init([[maybe_unused]]const std::string_view log_directory_path) {  // NOLINT
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
            return Status::ERR_INVALID_ARGS;
        }
    } else {
        /**
         * directory which has log_directory_path as a file path doesn't exist.
         * it can create.
         */
        boost::filesystem::create_directories(log_dir);
    }

#endif

    /**
     * If it already exists log files, it recoveries from those.
     */
#if defined(RECOVERY)
    Log::recovery_from_log();
#else
#if defined(CPR)
    if (boost::filesystem::exists(get_checkpoint_path())) {
        /**
         * If checkpoint of old database exists and it starts with no recovery, remove checkpoint to prevent confusing
         * between invalid checkpoint and valid checkpoint.
         */
        boost::filesystem::remove(get_checkpoint_path());
    }
#endif

    /**
     * pwal case : each log file is opened with truncating at init_kThreadTable func.
     */

#endif

    session_info_table::init_kThreadTable();
    epoch::invoke_epocher();
    snapshot_manager::invoke_snapshot_manager();

    yakushima::init();

#ifdef CPR
    cpr::invoke_checkpoint_thread();
#endif

    return Status::OK;
}

Status leave(Token const token) {  // NOLINT
    for (auto &&itr : session_info_table::get_thread_info_table()) {
        if (&itr == static_cast<session_info*>(token)) {
            if (itr.get_visible()) {
                itr.gc();
                yakushima::leave(static_cast<session_info*>(token)->get_yakushima_token());
                itr.set_tx_began(false);
                itr.set_visible(false);
                return Status::OK;
            }
            return Status::WARN_NOT_IN_A_SESSION;
        }
    }
    return Status::ERR_INVALID_ARGS;
}

void tx_begin(Token const token, bool const read_only) { // NOLINT
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) {
        /**
         * This func includes loading latest global epoch used for epoch-base resource management. This means that this
         * func is also bound of epoch-base resource management such as view management for gc to prevent segv. So this
         * func is called once by each tx.
         */
        ti->set_tx_began(true);
        ti->set_epoch(epoch::kGlobalEpoch.load(std::memory_order_acquire));
        ti->set_read_only(read_only);
#if defined(CPR)
        ti->update_pv();
#endif
    }
}

Status read_record(Record &res, const Record* const dest) {  // NOLINT
    tid_word f_check;
    tid_word s_check;  // first_check, second_check for occ

    f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));

    for (;;) {
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

        auto repeat_num{0};
        while (f_check.get_lock()) {
            _mm_pause();
            f_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
            Status s{check_concurrent_others_write()};
            if (s != Status::OK) return s;
            ++repeat_num;
            if (repeat_num > 100) return Status::WARN_CONCURRENT_INSERT;
        }

        Status s{check_concurrent_others_write()};
        if (s != Status::OK) return s;


        res.get_tuple() = dest->get_tuple();  // execute copy assign.

        s_check.set_obj(loadAcquire(dest->get_tidw().get_obj()));
        if (f_check == s_check) {
            break;
        }
        f_check = s_check;
    }

    res.set_tidw(f_check);
    return Status::OK;
}

void write_phase(session_info* const ti, const tid_word &max_r_set, const tid_word &max_w_set,
                 [[maybe_unused]]commit_property cp) {
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

    for (auto iws = ti->get_write_set().begin(); iws != ti->get_write_set().end();
         ++iws) {
        Record* rec_ptr = iws->get_rec_ptr();
        auto safely_snap_work = [&rec_ptr, &ti] {
            std::string_view old_value = rec_ptr->get_tuple().get_value();
            if (epoch::get_snap_epoch(ti->get_epoch()) != epoch::get_snap_epoch(rec_ptr->get_tidw().get_epoch())) {
                // update safely snap
                Record* new_rec = new Record(rec_ptr->get_tuple().get_key(), old_value); // NOLINT
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
                ti->get_gc_snap_cont().emplace_back(std::make_pair(ti->get_epoch(), new_rec));
            }
        };
        switch (iws->get_op()) {
            case OP_TYPE::INSERT: {
#ifdef CPR
                if (ti->get_phase() != cpr::phase::REST && rec_ptr->get_version() != (ti->get_version() + 1)) {
                    if (!rec_ptr->get_checkpointed()) {
                        rec_ptr->get_stable() = rec_ptr->get_tuple();
                        rec_ptr->get_stable_tidw() = max_tid;
                        rec_ptr->set_version(ti->get_version() + 1);
                    }
                }
#endif
                storeRelease(rec_ptr->get_tidw().get_obj(), max_tid.get_obj());
                break;
            }
            case OP_TYPE::UPDATE: {
                safely_snap_work();
                std::string* old_value{};
                std::string_view new_value_view = iws->get_tuple(iws->get_op()).get_value();
                rec_ptr->get_tuple().get_pimpl()->set_value(new_value_view.data(), new_value_view.size(), &old_value);
                if (old_value != nullptr) {
                    ti->get_gc_value_container().emplace_back(std::make_pair(old_value, ti->get_epoch()));
                } else {
                    /**
                     *  null insert is not expected.
                     */
                    SPDLOG_DEBUG("fatal error.");
                    exit(1);
                }
#ifdef CPR
                if (ti->get_phase() != cpr::phase::REST && rec_ptr->get_version() != (ti->get_version() + 1)) {
                    if (!rec_ptr->get_checkpointed()) {
                        rec_ptr->get_stable() = rec_ptr->get_tuple();
                        rec_ptr->get_stable_tidw() = max_tid;
                        rec_ptr->set_version(ti->get_version() + 1);
                    }
                }
#endif

                storeRelease(rec_ptr->get_tidw().get_obj(), max_tid.get_obj());
                break;
            }
            case OP_TYPE::DELETE: {
                safely_snap_work();
                tid_word delete_tid = max_tid;
                delete_tid.set_latest(false);
                delete_tid.set_absent(true);

                std::string_view key_view = rec_ptr->get_tuple().get_key();

                /**
                 * about removing index
                 */
#ifndef CPR
                /**
                 * case : no logging and pwal
                 */
#else
                // todo : sefely snap opt with cpr
                if (ti->get_phase() == cpr::phase::REST) {
                    /**
                     * This is in rest phase or in-progress phase, meaning checkpoint thread does not scan yet.
                     */
                    yakushima::remove(ti->get_yakushima_token(), key_view);
                    ti->get_gc_record_container().emplace_back(rec_ptr);
                } else {
                    /**
                     * This is in checkpointing phase (in-progress or wait-flush), meaning checkpoint thread may be scanning.
                     */
                    if (rec_ptr->get_checkpointed()) {
                        /**
                         * Checkpoint thread did process, so it can remove from index.
                         */
                        yakushima::remove(ti->get_yakushima_token(), key_view);
                        ti->get_gc_record_container().emplace_back(rec_ptr);
                    }
                    /**
                     * else : The check pointer is responsible for deleting from the index and registering garbage.
                     */
                }
#endif
                /**
                 * end about removing index
                 */

#ifdef CPR
                if (ti->get_phase() != cpr::phase::REST && rec_ptr->get_version() != (ti->get_version() + 1)) {
                    if (!rec_ptr->get_checkpointed()) {
                        rec_ptr->get_stable() = rec_ptr->get_tuple();
                        rec_ptr->get_stable_tidw() = delete_tid;
                        rec_ptr->set_version(ti->get_version() + 1);
                    }
                }
#endif
                if (rec_ptr->get_snap_ptr() == nullptr) {
                    // if no snapshot, it can immediately remove.
                    yakushima::remove(ti->get_yakushima_token(), key_view);
                    ti->get_gc_record_container().emplace_back(rec_ptr);
                    storeRelease(rec_ptr->get_tidw().get_obj(), delete_tid.get_obj());
                } else {
                    snapshot_manager::remove_rec_cont_mutex.lock();
                    snapshot_manager::remove_rec_cont.emplace_back(rec_ptr);
                    storeRelease(rec_ptr->get_tidw().get_obj(), delete_tid.get_obj());
                    snapshot_manager::remove_rec_cont_mutex.unlock();
                }
                break;
            }
            default:
                SPDLOG_DEBUG("fatal error.");
                std::abort();
        }
    }

}

}  // namespace shirakami::cc_silo_variant
