
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#include "concurrency_control/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

namespace shirakami::read_only_tx {

static inline void cleanup_process(session* const ti) {
    // local effect
    ti->clean_up();
}

Status abort(session* const ti) {
    // about transaction state
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

    // set transaction result
    ti->set_result(reason_code::USER_ABORT);

    // clean up
    cleanup_process(ti);
    return Status::OK;
}

void process_tx_state(session* const ti,
                      [[maybe_unused]] epoch::epoch_t const durable_epoch) {
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

Status commit(session* const ti) {
    // about transaction state
    process_tx_state(ti, ti->get_valid_epoch());

    // set transaction result
    ti->set_result(reason_code::UNKNOWN);

    // clean up
    cleanup_process(ti);
    return Status::OK;
}

Status tx_begin(session* const ti) {
    // set epoch
    auto ep = epoch::get_cc_safe_ss_epoch();
    ti->set_valid_epoch(ep);

    return Status::OK;
}

} // namespace shirakami::read_only_tx