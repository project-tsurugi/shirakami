
#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/lpwal.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h" // for size

#include "database/include/database.h"

#include "shirakami/log_record.h"

#include "limestone/api/write_version_type.h"

#include "glog/logging.h"

namespace shirakami::lpwal {

/**
  * @brief It executes log_channel_.add_entry for entire logs_.
  */
void add_entry_from_logs(handler& handle) {
    // send logs to set callback
    bool enable_callback{get_log_event_callback()};
    std::vector<shirakami::log_record> logs_for_callback; // NOLINT
    if (enable_callback) {
        logs_for_callback.reserve(handle.get_logs().size());
    }

    // send logs to limestone
    for (auto&& log_elem : handle.get_logs()) {
        if (log_elem.get_operation() == log_operation::DELETE) {
            // this is delete
            // todo for delete, wait for limestone impl
        } else {
            // this is write
            // now no source
            handle.get_log_channel_ptr()->add_entry(
                    static_cast<limestone::api::storage_id_type>(
                            log_elem.get_st()),
                    log_elem.get_key(), log_elem.get_val(),
                    limestone::api::write_version_type(
                            static_cast<limestone::api::epoch_t>(
                                    log_elem.get_wv()
                                            .get_major_write_version()),
                            static_cast<std::uint64_t>(
                                    log_elem.get_wv()
                                            .get_minor_write_version()))

            );
            if (enable_callback) {
                logs_for_callback.emplace_back(
                        log_elem.get_operation(), log_elem.get_key(),
                        log_elem.get_val(),
                        log_elem.get_wv().get_major_write_version(),
                        log_elem.get_wv().get_minor_write_version(),
                        log_elem.get_st());
            }
        }
    }

    // logging callback
    if (enable_callback) {
        get_log_event_callback()(handle.get_worker_number(),
                                 &(*logs_for_callback.begin()),
                                 &(*(logs_for_callback.end() - 1)));
    }

    handle.get_logs().clear();
    handle.set_min_log_epoch(0);
}

void daemon_work() {
    for (;;) {
        // sleep epoch time
        sleepMs(PARAM_EPOCH_TIME);

        // check fin
        if (get_stopping()) { break; }


        // do work
        for (auto&& es : session_table::get_session_table()) {
            auto oldest_log_epoch{es.get_lpwal_handle().get_min_log_epoch()};
            if (oldest_log_epoch != 0 &&
                oldest_log_epoch != epoch::get_global_epoch()) {
                flush_log(es.get_lpwal_handle());
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

void flush_log(handler& handle) {
    // this is called worker or daemon, so use try_lock
    if (handle.get_mtx_logs().try_lock()) {
        // register epoch before flush work
        auto ce{epoch::get_global_epoch()};

        if (!handle.get_logs().empty()) {
            handle.get_log_channel_ptr()->begin_session();
            add_entry_from_logs(handle);
            handle.get_log_channel_ptr()->end_session();
        }

        // register last flushed epoch
        handle.set_last_flushed_epoch(ce);

        handle.get_mtx_logs().unlock();
    }
}

void flush_remaining_log(bool& was_nothing) {
    was_nothing = true;
    for (auto&& es : session_table::get_session_table()) {
        if (!es.get_lpwal_handle().get_logs().empty()) {
            was_nothing = false;
            es.get_lpwal_handle().get_log_channel_ptr()->begin_session();
            add_entry_from_logs(es.get_lpwal_handle());
            es.get_lpwal_handle().get_log_channel_ptr()->end_session();
        }
    }
}

} // namespace shirakami::lpwal