

#include <cstdlib>
#include <cstring>
#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#include "database/include/thread_pool.h"

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

void fin([[maybe_unused]] bool force_shut_down_logging) try {
    if (!get_initialized()) { return; }

    /**
     * about back ground worker about commit
     * background worker about commit may access global data (object), so it 
     * must execute before cleanup environment.
     */
    VLOG(log_debug_timing_event)
            << log_location_prefix_timing_event << "shutdown:start_bg_commit";
    bg_work::bg_commit::fin();
    VLOG(log_debug_timing_event)
            << log_location_prefix_timing_event << "shutdown:end_bg_commit";

    // about datastore
#if defined(PWAL)
    VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                 << "shutdown:start_send_txlog_wait_durable";
    lpwal::fin(); // stop damon
    if (!force_shut_down_logging) {
        // flush remaining log
        lpwal::flush_remaining_log(); // (*1)
        epoch::epoch_t ce{epoch::get_global_epoch()};
        // wait durable above flushing
        for (;;) {
            if (epoch::get_durable_epoch() >= ce) { break; }
            _mm_pause();
        }
    }
    VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                 << "shutdown:end_send_txlog_wait_durable";
#endif

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
    datastore::get_datastore()->shutdown(); // this should after epoch::fin();
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
    auto *fast_shutdown = std::getenv("TSURUGI_FAST_SHUTDOWN");
    if (fast_shutdown != nullptr && std::strcmp(fast_shutdown, "1") == 0) {
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
    yakushima::fin();
    VLOG(log_debug_timing_event) << log_location_prefix_timing_event
                                 << "shutdown:end_shutdown_yakushima";

    //// about thread pool
    //VLOG(log_debug_timing_event) << log_location_prefix_timing_event
    //                             << "shutdown:start_shutdown_thread_pool";
    //thread_pool::fin();
    //VLOG(log_debug_timing_event) << log_location_prefix_timing_event
    //                             << "shutdown:end_shutdown_thread_pool";

    // clear flag
    set_initialized(false);
} catch (std::exception& e) {
    LOG(ERROR) << log_location_prefix << e.what();
    return;
}

} // namespace shirakami