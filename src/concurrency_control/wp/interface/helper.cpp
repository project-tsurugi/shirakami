

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/wp/include/epoch_internal.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/read_only_tx/include/read_only_tx.h"

#ifdef PWAL

#include "concurrency_control/wp/include/lpwal.h"

#include "datastore/limestone/include/datastore.h"

#include "limestone/api/datastore.h"

#endif

#include "shirakami/interface.h"

#include "boost/filesystem/path.hpp"

#include "glog/logging.h"

namespace shirakami {

Status check_before_write_ops(session* const ti, Storage const st,
                              OP_TYPE const op) {
    // check whether it is read only mode.
    if (ti->get_tx_type() == TX_TYPE::READ_ONLY) {
        // can't write in read only mode.
        return Status::WARN_ILLEGAL_OPERATION;
    }

    // check storage and wp data
    wp::wp_meta* wm{};
    auto rc{wp::find_wp_meta(st, wm)};
    if (rc == Status::WARN_NOT_FOUND) {
        // no storage.
        return Status::WARN_STORAGE_NOT_FOUND;
    }

    // long check
    if (ti->get_tx_type() == TX_TYPE::LONG) {
        if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
            // not in valid epoch.
            return Status::WARN_PREMATURE;
        }
        if (!ti->check_exist_wp_set(st)) {
            // can't write without wp.
            return Status::WARN_WRITE_WITHOUT_WP;
        }
        // insert and delete with read
        // may need forwarding
        rc = long_tx::wp_verify_and_forwarding(ti, wm);
        if (rc != Status::OK) { return rc; }
    } else if (ti->get_tx_type() == TX_TYPE::SHORT) {
        // check wp
        auto wps{wm->get_wped()};
        auto find_min_ep{wp::wp_meta::find_min_ep(wps)};
        if (find_min_ep != 0 && op != OP_TYPE::UPSERT) {
            // exist valid wp
            return Status::WARN_CONFLICT_ON_WRITE_PRESERVE;
        }
    }

    return Status::OK;
}

Status enter(Token& token) { // NOLINT
    Status ret_status = session_table::decide_token(token);
    if (ret_status != Status::OK) return ret_status;

    yakushima::Token kvs_token{};
    while (yakushima::enter(kvs_token) != yakushima::status::OK) {
        _mm_pause();
    }
    static_cast<session*>(token)->set_yakushima_token(kvs_token);

    return Status::OK;
}

void fin([[maybe_unused]] bool force_shut_down_logging) try {
    if (!get_initialized()) { return; }

    // about datastore
#if defined(PWAL)
    lpwal::fin(); // stop damon
    if (!force_shut_down_logging) {
        // flush remaining log
        bool was_nothing{false};
        lpwal::flush_remaining_log(was_nothing); // (*1)
        epoch::epoch_t ce{epoch::get_global_epoch()};
        // (*1)'s log must be before ce timing.
        if (!was_nothing) {
            // wait durable above flushing
            for (;;) {
                if (epoch::get_durable_epoch() >= ce) { break; }
                _mm_pause();
            }
        }
    }
    if (!lpwal::get_log_dir_pointed()) {
        // log dir was not pointed. So remove log dir
        lpwal::remove_under_log_dir();
    }
    lpwal::clean_up_metadata();
#endif

    // about tx engine
    garbage::fin();
    epoch::fin();
#ifdef PWAL
    datastore::get_datastore()->shutdown(); // this should after epoch::fin();
#endif
    delete_all_records(); // This should be before wp::fin();
    wp::fin();            // note: this use yakushima.

    // about index
    yakushima::fin();

    // clear flag
    set_initialized(false);
} catch (std::exception& e) {
    LOG(FATAL) << e.what();
    std::abort();
}

Status init([[maybe_unused]] database_options options) { // NOLINT
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    // about storage
    storage::init();

#if defined(PWAL)
    // check args
    std::string log_dir(options.get_log_directory_path());
    bool enable_true_log_nothing{false};
    if (log_dir.empty()) {
        if (options.get_open_mode() == database_options::open_mode::RESTORE) {
            // order to recover, but log_dir is nothing
            enable_true_log_nothing = true;
        }
        int tid = syscall(SYS_gettid); // NOLINT
        std::uint64_t tsc = rdtsc();
        log_dir = "/tmp/shirakami-" + std::to_string(tid) + "-" +
                  std::to_string(tsc);
        lpwal::set_log_dir_pointed(false);
        lpwal::set_log_dir(log_dir);
    } else {
        lpwal::set_log_dir(log_dir);
        lpwal::set_log_dir_pointed(true);
        // check exist
        boost::filesystem::path ldp{
                std::string(options.get_log_directory_path())};
        boost::system::error_code error;
        const bool result = boost::filesystem::exists(ldp, error);
        if (!result || error) {
            // exists
            if (options.get_open_mode() ==
                database_options::open_mode::CREATE) {
                // there are some data not expected.
                lpwal::set_log_dir(log_dir);
                lpwal::remove_under_log_dir();
            }
        }
    }

    // start datastore
    std::string data_location_str(log_dir);
    boost::filesystem::path data_location(data_location_str);
    std::vector<boost::filesystem::path> data_locations;
    data_locations.emplace_back(data_location);
    std::string metadata_dir{log_dir + "m"};
    boost::filesystem::path metadata_path(metadata_dir);
    datastore::start_datastore(
            limestone::api::configuration(data_locations, metadata_path));
    if (options.get_open_mode() != database_options::open_mode::CREATE &&
        !enable_true_log_nothing) {
        datastore::get_datastore()->recover(); // should execute before ready()
    }
    datastore::get_datastore()->add_persistent_callback(
            epoch::set_durable_epoch); // should execute before ready()
    /**
     * This executes create_channel and pass it to shirakami's executor.
     */
    datastore::init_about_session_table(log_dir);
    datastore::get_datastore()->ready();

#endif

    // about tx state
    TxState::init();

    // about cc
    session_table::init_session_table();
    //epoch::invoke_epoch_thread();

    // about index
    // pre condition : before wp::init() because wp::init() use yakushima function.
    yakushima::init();

    // about wp
    auto ret = wp::init();
    if (ret != Status::OK) { return ret; }

#ifdef PWAL
    // recover shirakami from datastore recovered.
    if (options.get_open_mode() != database_options::open_mode::CREATE &&
        !enable_true_log_nothing) {
        datastore::recovery_from_datastore();
        // logging the shirakami state after recovery
        datastore::scan_all_and_logging(); // todo remove?
    }
#endif

    // about epoch
    epoch::init();
    garbage::init();

#ifdef PWAL
    lpwal::init(); // start damon
#endif

    set_initialized(true); // about init command
    return Status::OK;
}

Status leave(Token const token) { // NOLINT
    for (auto&& itr : session_table::get_session_table()) {
        if (&itr == static_cast<session*>(token)) {
            if (itr.get_visible()) {
                // there may be halfway txs.
                shirakami::abort(token);

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

Status tx_begin(Token const token, TX_TYPE const tx_type,
                std::vector<Storage> write_preserve) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    if (!ti->get_tx_began()) {
        if (!write_preserve.empty()) {
            if (tx_type != TX_TYPE::LONG) {
                return Status::WARN_ILLEGAL_OPERATION;
            }
        }
        if (tx_type == TX_TYPE::LONG) {
            auto rc{long_tx::tx_begin(ti, std::move(write_preserve))};
            if (rc != Status::OK) {
                ti->process_before_finish_step();
                return rc;
            }
            ti->get_write_set().set_for_batch(true);
        } else if (tx_type == TX_TYPE::SHORT) {
            ti->get_write_set().set_for_batch(false);
        } else if (tx_type == TX_TYPE::READ_ONLY) {
            auto rc{read_only_tx::tx_begin(ti)};
            if (rc != Status::OK) {
                LOG(ERROR) << rc;
                ti->process_before_finish_step();
                return rc;
            }
        } else {
            LOG(ERROR) << "programming error";
            return Status::ERR_FATAL;
        }
        ti->set_tx_type(tx_type);
        ti->set_tx_began(true);
    } else {
        ti->process_before_finish_step();
        return Status::WARN_ALREADY_BEGIN;
    }

    ti->process_before_finish_step();
    return Status::OK;
}

Status read_record(Record* const rec_ptr, tid_word& tid, std::string& val,
                   bool const read_value = true) { // NOLINT
    tid_word f_check{};
    tid_word s_check{};

    f_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));

    for (;;) {
        auto return_some_others_write_status = [&f_check] {
            if (f_check.get_absent() && f_check.get_latest()) {
                return Status::WARN_CONCURRENT_INSERT;
            }
            if (f_check.get_absent() && !f_check.get_latest()) {
                return Status::WARN_NOT_FOUND;
            }
            return Status::WARN_CONCURRENT_UPDATE;
        };

#if PARAM_RETRY_READ > 0
        auto check_concurrent_others_write = [&f_check] {
            if (f_check.get_absent()) {
                if (f_check.get_latest()) {
                    return Status::WARN_CONCURRENT_INSERT;
                }
                return Status::WARN_NOT_FOUND;
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
            f_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
            Status s{check_concurrent_others_write()};
            if (s != Status::OK) return s;
            ++repeat_num;
#endif
        }

        if (f_check.get_absent()) { return Status::WARN_NOT_FOUND; }

        if (read_value) { rec_ptr->get_value(val); }
        s_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
        if (f_check == s_check) { break; }
        f_check = s_check;
    }

    tid = f_check;
    return Status::OK;
}

Status try_deleted_to_inserting([[maybe_unused]] TX_TYPE tp, // todo remove
                                Record* const rec_ptr, tid_word& found_tid) {
    tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
    // record found_tid
    found_tid = check;

    // point 1: pre-check
    if (check.get_latest() && check.get_absent()) {
        return Status::WARN_CONCURRENT_INSERT;
    }
    if (!check.get_absent()) { return Status::WARN_ALREADY_EXISTS; }
    // The page was deleted at point 1.

    // lock
    rec_ptr->get_tidw_ref().lock();

    // point 2: main check with lock
    tid_word tid{rec_ptr->get_tidw_ref()};
    if (tid.get_absent()) {
        // success
        tid.set_latest(true);
        rec_ptr->set_tid(tid);
        rec_ptr->get_tidw_ref().unlock();
        return Status::OK;
    }
    /**
      * The deleted page was changed to living page by someone between 
      * point 1 and point 2.
      */
    rec_ptr->get_tidw_ref().unlock();
    return Status::WARN_ALREADY_EXISTS;
}

#ifndef PWAL
void* get_datastore() { return nullptr; }
#endif

} // namespace shirakami