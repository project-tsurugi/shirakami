
#include <cmath>

#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/lpwal.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h" // for size

#include "database/include/database.h"
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
            add_entry(handle.get_log_channel_ptr(),
                      static_cast<limestone::api::storage_id_type>(
                              log_elem.get_st()),
                      log_elem.get_key(), log_elem.get_val(),
                      static_cast<limestone::api::epoch_t>(
                              log_elem.get_wv().get_major_write_version()),
                      static_cast<std::uint64_t>(
                              log_elem.get_wv().get_minor_write_version()));
            if (log_elem.get_wv().get_major_write_version() >
                handle.get_last_flushed_epoch()) {
                handle.set_last_flushed_epoch(
                        log_elem.get_wv().get_major_write_version());
            }
            if (enable_callback) {
                if (log_elem.get_st() < pow(2, 32)) { // TODO REMOVE // NOLINT
                    logs_for_callback.emplace_back(
                            log_elem.get_operation(), log_elem.get_key(),
                            log_elem.get_val(),
                            log_elem.get_wv().get_major_write_version(),
                            log_elem.get_wv().get_minor_write_version(),
                            log_elem.get_st());
                }
            }
        }
    }

    // logging callback
    if (enable_callback && !logs_for_callback.empty()) {
        get_log_event_callback()(handle.get_worker_number(),
                                 &*logs_for_callback.begin(),  // NOLINT
                                 &*logs_for_callback.begin() + // NOLINT
                                         logs_for_callback.size());
    }

    handle.get_logs().clear();
    handle.set_min_log_epoch(0);
}

void daemon_work() {
    epoch::epoch_t min_flush_ep{0};
    bool can_compute_min_flush_ep{true};
    for (;;) {
        // sleep epoch time
        sleepMs(PARAM_EPOCH_TIME);

        // check fin
        if (get_stopping()) { break; }


        // do work
        for (auto&& es : session_table::get_session_table()) {
            // flush work
            flush_log(&es);

            // compute durable epoch
            if (can_compute_min_flush_ep) {
                auto fe{es.get_lpwal_handle().get_last_flushed_epoch()};
                if (min_flush_ep == 0) {
                    min_flush_ep = fe;
                } else {
                    if (min_flush_ep > fe) { min_flush_ep = fe; }
                }
            }
        }
        if (min_flush_ep != 0) {
            // set durable epoch
            auto lep{epoch::get_durable_epoch()};
            if (lep >= min_flush_ep) {
                lpwal::set_durable_epoch(min_flush_ep - 1);
                can_compute_min_flush_ep = true;
                min_flush_ep = 0;
            } else {
                // stop compute because lep may not overtake min_flush_ep
                can_compute_min_flush_ep = false;
            }
        } else {
            can_compute_min_flush_ep = true;
            min_flush_ep = 0;
        }
    }
}

void init() {
    // initialize "some" global variables
    set_stopping(false);
    lpwal::set_durable_epoch(0);
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
        // register epoch before flush work (*1)
        auto ce{epoch::get_global_epoch()};

        if (handle.get_logs().empty()) {
            // optimizations
            if (ti->get_visible()) {
                // the session is opened
                if (ti->get_tx_began()) {
                    if (ti->get_tx_type() ==
                        transaction_options::transaction_type::SHORT) {
                        handle.set_last_flushed_epoch(ti->get_step_epoch());
                    } else if (ti->get_tx_type() ==
                               transaction_options::transaction_type::LONG) {
                        auto oep{ongoing_tx::get_lowest_epoch()};
                        if (oep == 0) {
                            // the ltx was committed without logging and no logs
                            handle.set_last_flushed_epoch(ce);
                        } else {
                            handle.set_last_flushed_epoch(oep);
                        }
                    } else if (ti->get_tx_type() ==
                               transaction_options::transaction_type::
                                       READ_ONLY) {
                        // read only
                        handle.set_last_flushed_epoch(ce);
                    } else {
                        // unreachable path
                        LOG(ERROR) << "programming error";
                    }
                } else {
                    handle.set_last_flushed_epoch(ce);
                }
            } else {
                // the session is not opened
                handle.set_last_flushed_epoch(ce);
            }
        } else {
            begin_session(handle.get_log_channel_ptr());
            add_entry_from_logs(handle);
            end_session(handle.get_log_channel_ptr());
        }

        handle.get_mtx_logs().unlock();
    }
}

void flush_remaining_log(bool& was_nothing) {
    was_nothing = true;
    for (auto&& es : session_table::get_session_table()) {
        if (!es.get_lpwal_handle().get_logs().empty()) {
            was_nothing = false;
            begin_session(es.get_lpwal_handle().get_log_channel_ptr());
            add_entry_from_logs(es.get_lpwal_handle());
            end_session(es.get_lpwal_handle().get_log_channel_ptr());
        }
    }
}

} // namespace shirakami::lpwal