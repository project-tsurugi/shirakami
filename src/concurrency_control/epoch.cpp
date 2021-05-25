/**
 * @file epoch.cpp
 * @brief Implementation about epoch
 */

#include <glog/logging.h>

#include "concurrency_control/include/epoch.h"

#include <xmmintrin.h>  // NOLINT

#include "clock.h"
#include "concurrency_control/include/session_info_table.h"
#include "tuple_local.h"  // sizeof(Tuple)

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

namespace shirakami::epoch {

bool check_epoch_loaded() {  // NOLINT
    epoch_t curEpoch = kGlobalEpoch.load(std::memory_order_acquire);

    for (auto &&itr : session_info_table::get_thread_info_table()) {  // NOLINT
        if (itr.get_visible() && itr.get_txbegan() && itr.get_epoch() != curEpoch) {
            return false;
        }
    }

    return true;
}

void epocher() {
    // initialization considering after fin()
    kGlobalEpoch.store(0, std::memory_order_release);
    while (likely(!kEpochThreadEnd.load(std::memory_order_acquire))) {
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
            if (kEpochThreadEnd.load(std::memory_order_acquire)) return;
            _mm_pause();
        }

        kGlobalEpoch++;
        kReclamationEpoch.store(kGlobalEpoch.load(std::memory_order_acquire) - 2, std::memory_order_release);

    }
}

}  // namespace shirakami::epoch
