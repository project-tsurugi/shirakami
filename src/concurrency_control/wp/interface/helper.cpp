

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"

#include "include/helper.h"

#include "concurrency_control/wp/include/epoch_internal.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status check_before_write_ops(session* const ti, Storage const st,
                              OP_TYPE const op) {
    // check whether it is read only mode.
    if (ti->get_read_only()) {
        // can't write in read only mode.
        return Status::WARN_ILLEGAL_OPERATION;
    }

    // batch check
    if (ti->get_tx_type() == TX_TYPE::LONG) {
        if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
            // not in valid epoch.
            return Status::WARN_PREMATURE;
        }
        if (!ti->check_exist_wp_set(st)) {
            // can't write without wp.
            return Status::WARN_WRITE_WITHOUT_WP;
        }
    } else if (ti->get_tx_type() == TX_TYPE::SHORT) {
        // check wp
        wp::wp_meta* wm{};
        auto rc{wp::find_wp_meta(st, wm)};
        if (rc == Status::WARN_NOT_FOUND) {
            // no storage.
            return Status::WARN_STORAGE_NOT_FOUND;
        }
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

void fin([[maybe_unused]] bool force_shut_down_cpr) try {
    if (!get_initialized()) { return; }

    // about engine
    garbage::fin();
    epoch::fin();
    delete_all_records(); // This should be before wp::fin();
    wp::fin();            // note: this use yakushima.

    // about index
    yakushima::fin();

    // clear flag
    set_initialized(false);
} catch (std::exception& e) {
    LOG(FATAL);
    std::abort();
}

Status
init([[maybe_unused]] bool enable_recovery,
     [[maybe_unused]] const std::string_view log_directory_path) { // NOLINT
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    // about tx state
    TxState::init();

    // about storage
    storage::init();

    // about cc
    session_table::init_session_table(enable_recovery);
    //epoch::invoke_epoch_thread();

    // about index
    // pre condition : before wp::init() because wp::init() use yakushima function.
    yakushima::init();

    // about wp
    auto ret = wp::init();
    if (ret != Status::OK) { return ret; }

    // about epoch
    epoch::init();
    garbage::init();

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

Status tx_begin(Token const token, bool const read_only, bool const for_batch,
                std::vector<Storage> write_preserve) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    if (!ti->get_tx_began()) {
        if (read_only && !write_preserve.empty()) {
            return Status::WARN_ILLEGAL_OPERATION;
        }
        if (for_batch) {
            ti->set_tx_type(TX_TYPE::LONG);
            auto rc{long_tx::tx_begin(ti, std::move(write_preserve))};
            if (rc != Status::OK) {
                ti->process_before_finish_step();
                return rc;
            }
        } else {
            ti->set_tx_type(TX_TYPE::SHORT);
        }
        ti->set_tx_began(true);
        ti->set_read_only(read_only);
        ti->get_write_set().set_for_batch(for_batch);
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
                return Status::WARN_CONCURRENT_DELETE;
            }
            return Status::WARN_CONCURRENT_UPDATE;
        };

#if PARAM_RETRY_READ > 0
        auto check_concurrent_others_write = [&f_check] {
            if (f_check.get_absent()) {
                if (f_check.get_latest()) {
                    return Status::WARN_CONCURRENT_INSERT;
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
            f_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
            Status s{check_concurrent_others_write()};
            if (s != Status::OK) return s;
            ++repeat_num;
#endif
        }

        if (f_check.get_absent()) { return Status::WARN_CONCURRENT_DELETE; }

        if (read_value) { rec_ptr->get_value(val); }
        s_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
        if (f_check == s_check) { break; }
        f_check = s_check;
    }

    tid = f_check;
    return Status::OK;
}

Status try_deleted_to_inserted(Record* const rec_ptr,
                               std::string_view const val) {
    tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
    // point 1
    if (check.get_latest() && check.get_absent()) {
        return Status::WARN_CONCURRENT_INSERT;
    }
    if (!check.get_absent()) { return Status::WARN_ALREADY_EXISTS; }
    // The page was deleted at point 1.

    rec_ptr->get_tidw_ref().lock();
    // point 2
    tid_word tid{rec_ptr->get_tidw_ref()};
    if (tid.get_absent()) {
        // success
        tid.set_latest(true);
        rec_ptr->set_tid(tid);
        rec_ptr->set_value(val);
        return Status::OK;
    }
    /**
      * The deleted page was changed to living page by someone between 
      * point 1 and point 2.
      */
    rec_ptr->get_tidw_ref().unlock();
    return Status::WARN_ALREADY_EXISTS;
}

} // namespace shirakami