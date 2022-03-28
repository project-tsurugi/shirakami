
#include "concurrency_control/wp/include/long_tx.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"

#include "concurrency_control/include/tuple_local.h"

namespace shirakami::long_tx {

Status tx_begin(session* const ti,
                std::vector<Storage> write_preserve) { // NOLINT
    // get wp mutex
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get batch id
    auto batch_id = shirakami::wp::long_tx::get_counter();

    // compute future epoch
    auto valid_epoch = epoch::get_global_epoch() + 1;

    // do write preserve
    if (!write_preserve.empty()) {
        auto rc{wp::write_preserve(ti, std::move(write_preserve), batch_id,
                                   valid_epoch)};
        if (rc != Status::OK) { return rc; }
    }

    // inc batch counter
    // after deciding success
    wp::long_tx::set_counter(batch_id + 1);

    ti->set_batch_id(batch_id);
    ongoing_tx::push({valid_epoch, batch_id});
    ti->set_tx_type(TX_TYPE::LONG);
    ti->set_valid_epoch(valid_epoch);

    return Status::OK;
    // dtor : release wp_mutex
}

Status version_function([[maybe_unused]] Record* rec,
                        [[maybe_unused]] epoch::epoch_t ep,
                        [[maybe_unused]] version*& ver) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami::long_tx