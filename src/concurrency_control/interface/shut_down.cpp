

#include <cstdlib>
#include <cstring>
#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#include "database/include/database.h"
#include "database/include/logging.h"
#include "database/include/thread_pool.h"
#include "database/include/tx_state_notification.h"

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

static inline bool is_fast_shutdown() {
    // check environ "TSURUGI_FAST_SHUTDOWN" first
    if (auto* fast_shutdown_envstr = std::getenv("TSURUGI_FAST_SHUTDOWN");
        fast_shutdown_envstr != nullptr && *fast_shutdown_envstr != '\0') {
        return std::strcmp(fast_shutdown_envstr, "1") == 0;
    }
    // use default value from build parameter
#if defined(TSURUGI_FAST_SHUTDOWN_ON)
    return true;
#else
    return false;
#endif
}

void fin_body([[maybe_unused]] bool force_shut_down_logging) try {
    if (!get_initialized()) { return; }
    // set flag
    set_is_shutdowning(true);

    if (get_used_database_options().get_open_mode() !=
        database_options::open_mode::MAINTENANCE) {

        /**
         * about back ground worker about commit
         * background worker about commit may access global data (object), so it
         * must execute before cleanup environment.
         */
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:start_bg_commit";
        bg_work::bg_commit::fin();
        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event << "shutdown:end_bg_commit";

        // about datastore
#if defined(PWAL)
        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event
                << "shutdown:start_send_txlog_wait_durable";
        lpwal::fin(); // stop damon
        if (!force_shut_down_logging) {
            // flush remaining log
            lpwal::flush_remaining_log(); // (*1)
            epoch::epoch_t ce{epoch::get_global_epoch()};
            // wait durable above flushing
            for (;;) {
                if (epoch::get_datastore_durable_epoch() >= ce) { break; }
                _mm_pause();
            }
        }
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:end_send_txlog_wait_durable";

#endif
        // about callbacks
        clear_durability_callbacks();

        // about tx engine
        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event << "shutdown:start_gc";
        garbage::fin();
        VLOG(log_debug_timing_event)
                << log_location_prefix_timing_event << "shutdown:end_gc";
        epoch::fin();
#ifdef PWAL
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:start_shutdown_datastore";
        datastore::get_datastore()
                ->shutdown(); // this should after epoch::fin();
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:end_shutdown_datastore";
        // cleanup about limestone
        if (!lpwal::get_log_dir_pointed()) {
            // log dir was not pointed. So remove log dir
            VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                         << "shutdown:start_cleanup_logdir";
            lpwal::remove_under_log_dir();
            VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                         << "shutdown:end_cleanup_logdir";
        }
        lpwal::clean_up_metadata();
#endif
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:start_delete_all_records";
        bool fast_shutdown = is_fast_shutdown();
        if (fast_shutdown) {
            LOG(INFO) << log_location_prefix << "skipped delete_all_records";
        } else {
            delete_all_records(); // This should be before wp::fin();
        }
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:end_delete_all_records";
        wp::fin();      // note: this use yakushima.
        storage::fin(); // note: this use yakushima. delete meta storage.

        // about index
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:start_shutdown_yakushima";
        if (fast_shutdown) {
            yakushima::storage::get_storages()->store_root_ptr(nullptr);
            LOG(INFO) << log_location_prefix << "skipping yakushima destroy";
        }
        yakushima::fin();
        VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                     << "shutdown:end_shutdown_yakushima";

        //// about thread pool
        //VLOG(log_debug_timing_event) << log_location_prefix_timing_event
        //                             << "shutdown:start_shutdown_thread_pool";
        //thread_pool::fin();
        //VLOG(log_debug_timing_event) << log_location_prefix_timing_event
        //                             << "shutdown:end_shutdown_thread_pool";

        // about read area
        read_plan::fin();
    }

    // set flag
    set_is_shutdowning(false);

    // clear flag
    set_initialized(false);
} catch (std::exception& e) {
    LOG_FIRST_N(ERROR, 1) << log_location_prefix << e.what();
    return;
}

void fin([[maybe_unused]] bool force_shut_down_logging) {
    shirakami_log_entry << "fin, force_shut_down_logging: "
                        << force_shut_down_logging;
    fin_body(force_shut_down_logging);
    shirakami_log_exit << "fin";
}

} // namespace shirakami
