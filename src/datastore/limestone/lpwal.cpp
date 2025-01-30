
#include <cmath>

#include "clock.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/lpwal.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"

#include "storage.h"

#include "database/include/database.h"
#include "database/include/tx_state_notification.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#include "shirakami/log_record.h"
#include "shirakami/logging.h"

#include "limestone/api/write_version_type.h"

#include "glog/logging.h"

namespace shirakami::lpwal {

/**
 * @brief It executes log_channel_.add_entry for entire logs_.
 */
void add_entry_from_logs(handler& handle) {
    // send logs to datastore
    for (auto&& log_elem : handle.get_logs()) {
        if (log_elem.get_operation() == log_operation::DELETE) {
            // delete
            remove_entry(handle.get_log_channel_ptr(),
                         static_cast<limestone::api::storage_id_type>(
                                 log_elem.get_st()),
                         log_elem.get_key(),
                         static_cast<limestone::api::epoch_t>(
                                 log_elem.get_wv().get_major_write_version()),
                         static_cast<std::uint64_t>(
                                 log_elem.get_wv().get_minor_write_version()));
        } else if (log_elem.get_operation() == log_operation::ADD_STORAGE) {
            // add storage
            add_storage(handle.get_log_channel_ptr(),
                         static_cast<limestone::api::storage_id_type>(
                                 log_elem.get_st()),
                         static_cast<limestone::api::epoch_t>(
                                 log_elem.get_wv().get_major_write_version()),
                         static_cast<std::uint64_t>(
                                 log_elem.get_wv().get_minor_write_version()));
        } else if (log_elem.get_operation() == log_operation::REMOVE_STORAGE) {
            // remove storage
            remove_storage(handle.get_log_channel_ptr(),
                         static_cast<limestone::api::storage_id_type>(
                                 log_elem.get_st()),
                         static_cast<limestone::api::epoch_t>(
                                 log_elem.get_wv().get_major_write_version()),
                         static_cast<std::uint64_t>(
                                 log_elem.get_wv().get_minor_write_version()));
        } else {
            // update / insert / upsert
            add_entry(handle.get_log_channel_ptr(),
                      static_cast<limestone::api::storage_id_type>(
                              log_elem.get_st()),
                      log_elem.get_key(), log_elem.get_val(),
                      static_cast<limestone::api::epoch_t>(
                              log_elem.get_wv().get_major_write_version()),
                      static_cast<std::uint64_t>(
                              log_elem.get_wv().get_minor_write_version()),
                      log_elem.get_lobs());
        }
    }

    handle.get_logs().clear();
    handle.set_min_log_epoch(0);
}

void daemon_work() {
    for (;;) {
        // sleep epoch time
        sleepUs(epoch::get_global_epoch_time_us());

        // check fin
        if (get_stopping()) { break; }


        // do work
        for (auto&& es : session_table::get_session_table()) {
            // FIXME: use durable epoch instead of min_log_epoch (this is write version)
            // copied from short_tx::commit
            // flush work
            auto oldest_log_epoch{es.get_lpwal_handle().get_min_log_epoch()};
            if (oldest_log_epoch != 0 && // mean the wal buffer is not empty.
                oldest_log_epoch != epoch::get_global_epoch()) {
                // should flush
                flush_log(&es);
            }
        }
    }
}

void init() {
    // initialize "some" global variables
    set_stopping(false);
    // start damon thread
    daemon_thread_ = std::thread(daemon_work);
}

void fin() {
    // issue signal for daemon
    set_stopping(true);

    // join damon thread
    daemon_thread_.join();

    // clean up signal
    set_stopping(false);
}

void handler::begin_session() {
    // assert begun_session_ = false
    shirakami::begin_session(log_channel_ptr_);
    // TODO: global epoch is approximate value for durable epoch, but is guaranteed to be >= real durable epoch.
    // if the exact value becomes available, change this to use that.
    // eg. if limestone::log_chanell::begin_session returns the exact value in the future,
    //     or shirakami controls the timing of begin_session/switch_epoch
    durable_epoch_ = epoch::get_global_epoch();
    begun_session_ = true;
}

void handler::end_session() {
    // assert begun_session_ = true
    shirakami::end_session(log_channel_ptr_);
    begun_session_ = false;
}

// @pre take mtx of logs
void flush_log_and_end_session_if_exists(lpwal::handler& handle) {
    if (!handle.get_logs().empty()) {
        if (!handle.get_begun_session()) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "lpwal worker#" << handle.get_worker_number()
                                  << ": session should be begun here";
            handle.begin_session();
        }
        add_entry_from_logs(handle);
        handle.end_session();
    }
    if (handle.get_begun_session()) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "lpwal worker#" << handle.get_worker_number()
                              << ": session should not be begun here";
        handle.end_session();
    }
}

void flush_log(Token token) {
    auto* ti = static_cast<session*>(token);
    auto& handle = ti->get_lpwal_handle();
    // this is called worker or daemon, so use try_lock
    if (handle.get_mtx_logs().try_lock()) {
        flush_log_and_end_session_if_exists(handle);
        handle.get_mtx_logs().unlock();
    }
}

void flush_remaining_log() {
    for (auto&& es : session_table::get_session_table()) {
        auto& handle = es.get_lpwal_handle();
        std::unique_lock lk{handle.get_mtx_logs()};
        flush_log_and_end_session_if_exists(handle);
    }
}

} // namespace shirakami::lpwal
