
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#include "concurrency_control/include/long_tx.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

namespace shirakami::read_only_tx {

static inline void cleanup_process(session* const ti) {
    // global effect
    ongoing_tx::remove_id(ti->get_long_tx_id());

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
    // exclude long tx's coming and epoch update
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get long tx id
    auto long_tx_id = shirakami::wp::long_tx::get_counter();

    // compute future epoch
    auto ce = epoch::get_global_epoch(); // this must be before (*1)
    {
        std::lock_guard<std::shared_mutex> lk{ongoing_tx::get_mtx()};
        auto valid_epoch = ongoing_tx::get_lowest_epoch(); // (*1)
        if (valid_epoch == 0) {
            // no long tx
            valid_epoch = ce + 1;
        }

        // inc long tx counter
        // after deciding success
        wp::long_tx::set_counter(long_tx_id + 1);

        // set metadata
        ti->set_long_tx_id(long_tx_id);
        ti->set_valid_epoch(valid_epoch);
        ongoing_tx::push_bringing_lock({valid_epoch, long_tx_id, ti});
    }
    return Status::OK;
    // dtor : release wp_mutex
}

} // namespace shirakami::read_only_tx