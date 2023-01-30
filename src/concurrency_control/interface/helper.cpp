

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#include "database/include/logging.h"

#ifdef PWAL

#include "concurrency_control/include/lpwal.h"

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#include "limestone/api/datastore.h"

#endif

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "boost/filesystem/path.hpp"

#include "glog/logging.h"

namespace shirakami {

Status check_constraint_key_length(std::string_view const key) {
    // check constraint: key
    if (key.size() > 35 * 1024) {
        // we can't control over 35KB key.
        return Status::WARN_INVALID_KEY_LENGTH;
    }

    return Status::OK;
}

Status check_before_write_ops(session* const ti, Storage const st,
                              std::string_view const key, OP_TYPE const op) {
    // check whether it is read only mode.
    if (ti->get_tx_type() == transaction_options::transaction_type::READ_ONLY) {
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
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
            // not in valid epoch.
            return Status::WARN_PREMATURE;
        }
        if (!ti->check_exist_wp_set(st)) {
            // can't write without wp.
            return Status::WARN_WRITE_WITHOUT_WP;
        }
        if (op != OP_TYPE::UPSERT) {
            // insert and delete with read
            // may need forwarding
            rc = long_tx::wp_verify_and_forwarding(ti, wm, key);
            if (rc != Status::OK) { return rc; }
        }
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::SHORT) {
        // check wp
        auto wps{wm->get_wped()};
        auto find_min_ep{wp::wp_meta::find_min_ep(wps)};
        if (find_min_ep != 0 && op != OP_TYPE::UPSERT) {
            // exist valid wp
            //ti->get_result_info().set_reason_code(
            //        reason_code::CC_OCC_WP_VERIFY);
            //ti->get_result_info().set_key_storage_name(key, st);
            return Status::WARN_CONFLICT_ON_WRITE_PRESERVE;
        }
    }

    return Status::OK;
}

Status read_record(Record* const rec_ptr, tid_word& tid, std::string& val,
                   bool const read_value = true) { // NOLINT
    tid_word f_check{};
    tid_word s_check{};

    f_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));

    // try atomic load payload
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
            if (logging::get_enable_logging_detail_info()) {
                // logging detail info
                DVLOG(log_trace)
                        << logging::log_location_prefix
                        << "start wait for locked record. key is " +
                                   std::string(rec_ptr->get_key_view());
            }
#if PARAM_RETRY_READ == 0
            if (logging::get_enable_logging_detail_info()) {
                // logging detail info
                DVLOG(log_trace)
                        << logging::log_location_prefix
                        << "finish wait for locked record. key is " +
                                   std::string(rec_ptr->get_key_view());
            }
            return return_some_others_write_status();
#else
            if (repeat_num >= PARAM_RETRY_READ) {
                if (logging::get_enable_logging_detail_info()) {
                    // logging detail info
                    DVLOG(log_trace)
                            << logging::log_location_prefix
                            << "finish wait for locked record. key is " +
                                       std::string(rec_ptr->get_key_view());
                }
                return return_some_others_write_status();
            }
            _mm_pause();
            f_check.set_obj(loadAcquire(rec_ptr->get_tidw_ref().get_obj()));
            Status s{check_concurrent_others_write()};
            if (s != Status::OK) {
                if (logging::get_enable_logging_detail_info()) {
                    // logging detail info
                    DVLOG(log_trace)
                            << logging::log_location_prefix
                            << "finish wait for locked record. key is " +
                                       std::string(rec_ptr->get_key_view());
                }
                return s;
            }
            ++repeat_num;
#endif
        }
        if (logging::get_enable_logging_detail_info()) {
            // logging detail info
            DVLOG(log_trace) << logging::log_location_prefix
                             << "finish wait for locked record. key is " +
                                        std::string(rec_ptr->get_key_view());
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

Status try_deleted_to_inserting(Storage st, std::string_view key,
                                Record* const rec_ptr, tid_word& found_tid) {
    tid_word check{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
    // record found_tid
    found_tid = check;

    // point 1: pre-check
    if (check.get_latest() && check.get_absent()) {
        // Someone is inserting the record.
        return Status::WARN_CONCURRENT_INSERT;
    }
    if (!check.get_absent()) {
        // not inserting, and it exists
        return Status::WARN_ALREADY_EXISTS;
    }
    // The page was deleted at point 1.

    // lock
    rec_ptr->get_tidw_ref().lock();

    // check: it can reach in index(i.e. not GCed)
    Record* found_rec_ptr{};
    if (Status::WARN_NOT_FOUND == get<Record>(st, key, found_rec_ptr) ||
        rec_ptr != found_rec_ptr) {
        rec_ptr->get_tidw_ref().unlock();
        return Status::WARN_NOT_FOUND;
    }

    // point 2: main check with lock
    tid_word tid{rec_ptr->get_tidw_ref()};
    if (tid.get_absent() && !tid.get_latest()) {
        // success, the record is deleted
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