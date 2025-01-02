
#include <algorithm>
#include <map>
#include <string_view>
#include <vector>

#include "atomic_wrapper.h"
#include "storage.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "database/include/logging.h"

#include "index/yakushima/include/interface.h"

#include "glog/logging.h"

namespace shirakami::long_tx {

// ==============================
// functions declared at header
Status check_wait_for_preceding_bt(session* const ti) {
    Status rc{};
    if (ongoing_tx::exist_wait_for(ti, rc)) {
        return Status::WARN_WAITING_FOR_OTHER_TX;
    }
    return rc;
}

static void process_tx_state(session* ti,
                             [[maybe_unused]] epoch::epoch_t durable_epoch) {
    if (ti->get_has_current_tx_state_handle()) {
#ifdef PWAL
        // this tx state is checked
        ti->get_current_tx_state_ptr()->set_durable_epoch(durable_epoch);
        ti->get_current_tx_state_ptr()->set_kind(
                TxState::StateKind::WAITING_DURABLE);
#else
        ti->get_current_tx_state_ptr()->set_kind(TxState::StateKind::DURABLE);
#endif
    }
}

static void update_read_area(session* const ti) {
    if (ti->get_ltx_storage_read_set().empty()) {
        // write only tx
        read_plan::add_elem(ti->get_long_tx_id(),
                            {{storage::dummy_storage}, {}});
        return;
    }

    // update
    read_plan::plist_type plist;
    read_plan::nlist_type nlist;
    for (auto&& elem : ti->get_ltx_storage_read_set()) {
        plist.insert(std::make_tuple(elem.first, true, std::get<0>(elem.second),
                                     std::get<1>(elem.second),
                                     std::get<2>(elem.second),
                                     std::get<3>(elem.second)));
    }
    read_plan::add_elem(ti->get_long_tx_id(), plist, nlist);
}

Status check_commit(Token const token) { // NOLINT
    return Status::ERR_NOT_IMPLEMENTED;
}

// ==============================

} // namespace shirakami::long_tx
