//
// Created by thawk on 2020/10/30.
//

#include "concurrency_control/silo_variant/include/session_info_table.h"

#include "fault_tolerance/include/cpr.h"

using namespace shirakami::cc_silo_variant;
using namespace shirakami::cc_silo_variant::epoch;

namespace shirakami::cpr {

void checkpoint_thread() {
    while (likely(kCheckPointThreadEnd.load(std::memory_order_acquire))) {

        // Commit() phase
        cpr::global_phase_version::set_gp(cpr::phase::PREPARE);
        kGlobalEpoch++;
        for (auto &&elem : session_info_table::get_thread_info_table()) {
            if (elem.get_visible()) {
            }
        }
        // PrepareToInProg() phase
        cpr::global_phase_version::set_gp(cpr::phase::IN_PROGRESS);

        // InProgToWaitFlush() phase
        cpr::global_phase_version::set_gp(cpr::phase::WAIT_FLUSH);

        // Atomically set global phase (rest) and increment version.
        cpr::global_phase_version::set_rest();
    }
}

}