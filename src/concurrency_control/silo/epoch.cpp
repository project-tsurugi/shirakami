/**
 * @file epoch.cpp
 * @brief Implementation about epoch
 */

#include <glog/logging.h>

#include "include/epoch.h"

#ifdef WP

#include "concurrency_control/include/wp.h"

#endif

#include <xmmintrin.h> // NOLINT

#include "clock.h"
#include "include/session_table.h"
#include "include/tuple_local.h" // sizeof(Tuple)

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

namespace shirakami::epoch {

bool check_epoch_loaded() { // NOLINT
    epoch_t curEpoch = get_global_epoch(); 

    for (auto&& itr : session_table::get_session_table()) { // NOLINT
        if (itr.get_visible() && itr.get_txbegan() && itr.get_epoch() != curEpoch) {
            return false;
        }
    }

    return true;
}

void epocher() {
    // initialization considering after fin()
    set_global_epoch(0);
    while (likely(!get_epoch_thread_end())) {
        /*
         * Increment global epoch in each PARAM_EPOCH_TIME [ms] (default: 40).
         */
        sleepMs(PARAM_EPOCH_TIME);
        /**
         * To increment global epoch, all the worker-threads need to read the latest one.
         * check_epoch_loaded() checks whether the
         * latest global epoch is read by all the threads
         */
        while (!check_epoch_loaded()) {
            if (get_epoch_thread_end()) return;
            _mm_pause();
        }

#ifdef WP
#if WP_LEVEL == 0
        // block batch
        std::unique_lock<std::mutex> get_lock{wp::get_wp_mutex()};
#endif
#endif

        kGlobalEpoch++;

#ifdef WP
#if WP_LEVEL == 0
        set_reclamation_epoch(get_global_epoch() - 2);
#else
        set_reclamation_epoch(get_global_epoch() - 2);
#endif
#endif

        // unblock batch
        // dtor get_lock
    }
}

void invoke_epocher() {
    // It may be redundant, but needs to restore if this is called after fin in the same program.
    set_epoch_thread_end(false);
    set_reclamation_epoch(-2);

    kEpochThread = std::thread(epocher);
}

} // namespace shirakami::epoch
