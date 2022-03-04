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

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

namespace shirakami::epoch {

bool check_epoch_loaded() { // NOLINT
    epoch_t curEpoch = get_global_epoch(); 

    for (auto&& itr : session_table::get_session_table()) { // NOLINT
        if (itr.get_visible() && itr.get_tx_began() && itr.get_epoch() != curEpoch) {
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

        kGlobalEpoch++;
    }
}

void invoke_epocher() {
    // It may be redundant, but needs to restore if this is called after fin in the same program.
    set_epoch_thread_end(false);
    set_reclamation_epoch(-2);

    kEpochThread = std::thread(epocher);
}

} // namespace shirakami::epoch
