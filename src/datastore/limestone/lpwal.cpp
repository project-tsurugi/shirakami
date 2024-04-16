
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
        } else {
            // update / insert / upsert
            add_entry(handle.get_log_channel_ptr(),
                      static_cast<limestone::api::storage_id_type>(
                              log_elem.get_st()),
                      log_elem.get_key(), log_elem.get_val(),
                      static_cast<limestone::api::epoch_t>(
                              log_elem.get_wv().get_major_write_version()),
                      static_cast<std::uint64_t>(
                              log_elem.get_wv().get_minor_write_version()));
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
            // flush work
            flush_log(&es);
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

void flush_log(Token token) {
    auto* ti = static_cast<session*>(token);
    auto& handle = ti->get_lpwal_handle();
    // this is called worker or daemon, so use try_lock
    if (handle.get_mtx_logs().try_lock()) {
        // flush log if exist
        if (!handle.get_logs().empty()) {
            begin_session(handle.get_log_channel_ptr());
            add_entry_from_logs(handle);
            end_session(handle.get_log_channel_ptr());
        }

        handle.get_mtx_logs().unlock();
    }
}

void flush_remaining_log() {
    for (auto&& es : session_table::get_session_table()) {
        if (!es.get_lpwal_handle().get_logs().empty()) {
            begin_session(es.get_lpwal_handle().get_log_channel_ptr());
            add_entry_from_logs(es.get_lpwal_handle());
            end_session(es.get_lpwal_handle().get_log_channel_ptr());
        }
    }
}

} // namespace shirakami::lpwal