/**
 * @file helper.cpp
 */

#include <glog/logging.h>

#include "storage.h"

#include "include/helper.h"

#include "concurrency_control/silo/include/cleanup_manager.h"
#include "concurrency_control/silo/include/garbage_manager.h"
#include "concurrency_control/silo/include/session_table.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#if defined(PWAL) || defined(CPR)

#include "fault_tolerance/include/log.h"

#endif

#include "shirakami/interface.h"

#include "glog/logging.h"

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

    LOG(INFO) << "shirakami: initialize shirakami, recovery mode: "
              << enable_recovery
              << ", tx log directory path: " << log_directory_path;

    // about storage
    storage::init();

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

    // set checkpoint path
    {
        std::string log_dir = Log::get_kLogDirectory();
        log_dir.append("/checkpoint");
        cpr::set_checkpoint_path(log_dir);
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
    if (enable_recovery) { cpr::create_checkpoint(); }

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
                // there may be halfway txs.
                abort(token);

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

} // namespace shirakami
