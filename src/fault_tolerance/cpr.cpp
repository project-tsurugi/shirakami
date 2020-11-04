//
// Created by thawk on 2020/10/30.
//

#include "concurrency_control/silo_variant/include/session_info_table.h"

#include "fault_tolerance/include/cpr.h"

#include "clock.h"

using namespace shirakami::cc_silo_variant;
using namespace shirakami::cc_silo_variant::epoch;

namespace shirakami::cpr {

void checkpoint_thread() {
    auto wait_worker = [](phase new_phase) {
        bool continue_loop{};
        do {
            continue_loop = false;
            for (auto &&elem : session_info_table::get_thread_info_table()) {
                if (elem.get_visible() && elem.get_phase() == new_phase) {
                    continue;
                }
                continue_loop = true;
                break;
            }
        } while (continue_loop);
    };

    while (likely(kCheckPointThreadEnd.load(std::memory_order_acquire))) {
        sleepMs(PARAM_CHECKPOINT_REST_EPOCH);

        /**
         * PrepareToInProg() phase.
         * Originally, there are 4 phase : rest, prepare, in_progress, wait_flush.
         * But in shirakami, it removes prepare phase to improve performance.
         */
        cpr::global_phase_version::set_gp(cpr::phase::IN_PROGRESS);
        wait_worker(phase::IN_PROGRESS);

        // InProgToWaitFlush() phase
        cpr::global_phase_version::set_gp(cpr::phase::WAIT_FLUSH);
        checkpointing();

        // Atomically set global phase (rest) and increment version.
        cpr::global_phase_version::set_rest();
    }
}

void checkpointing() {
    /**
     * todo
     * 1. If checkpointing is in progress, Deleted record after logical consistency point  must be observable from this
     * thread.
     */
#ifdef INDEX_KOHLER_MASSTREE
    /**
     * todo : kohler masstree version
     */
#elif INDEX_YAKUSHIMA
    std::vector<std::pair<Record**, std::size_t>> scan_buf;
    yakushima::scan({}, yakushima::scan_endpoint::INF, {}, yakushima::scan_endpoint::INF, scan_buf); // NOLINT

    for (auto &&itr : scan_buf) {
        Record* rec = *itr.first;

    }
#endif
}

}