
#include "concurrency_control/wp/interface/read_only_tx/include/read_only_tx.h"

#include "concurrency_control/wp/include/long_tx.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

namespace shirakami::read_only_tx {

static inline void cleanup_process(session* const ti) {
    // global effect
    ongoing_tx::remove_id(ti->get_long_tx_id());

    // local effect
    ti->clean_up();
}

Status abort([[maybe_unused]] session* ti) {
    // about transaction state
    ti->set_tx_state_if_valid(TxState::StateKind::ABORTED);

    // clean up
    cleanup_process(ti);
    return Status::OK;
}

Status commit([[maybe_unused]] session* ti) {
    // about transaction state
    // todo fix
    ti->set_tx_state_if_valid(TxState::StateKind::DURABLE);

    // clean up
    cleanup_process(ti);
    return Status::OK;
}

Status tx_begin(session* const ti) {
    // exclude long tx's coming
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get long tx id
    auto long_tx_id = shirakami::wp::long_tx::get_counter();

    // compute future epoch
    auto ce = epoch::get_global_epoch(); // this must be before (*1)
    auto valid_epoch = ongoing_tx::get_lowest_epoch(); // (*1)
    if (valid_epoch == 0) {
        // no long tx
        valid_epoch = ce;
    }

    // inc long tx counter
    // after deciding success
    wp::long_tx::set_counter(long_tx_id + 1);

    // set metadata
    ti->set_long_tx_id(long_tx_id);
    ti->set_valid_epoch(valid_epoch);
    ongoing_tx::push({valid_epoch, long_tx_id});

    return Status::OK;
    // dtor : release wp_mutex
}

} // namespace shirakami::read_only_tx