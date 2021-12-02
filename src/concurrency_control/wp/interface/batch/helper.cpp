
#include "concurrency_control/wp/include/batch.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "concurrency_control/wp/include/tuple_local.h"

namespace shirakami::batch {

Status tx_begin(session* const ti,
                std::vector<Storage> write_preserve) { // NOLINT
    // get wp mutex
    auto wp_mutex = std::unique_lock<std::mutex>(wp::get_wp_mutex());

    // get batch id
    auto batch_id = shirakami::wp::batch::get_counter();

    // compute future epoch
    auto valid_epoch = epoch::get_global_epoch() + 1;

    // do write preserve
    auto rc{wp::write_preserve(ti, std::move(write_preserve), batch_id, valid_epoch)};
    if (rc != Status::OK) { return rc; }

    // inc batch counter
    // after deciding success
    wp::batch::set_counter(batch_id + 1);

    ti->set_batch_id(batch_id);
    ti->set_mode(tx_mode::BATCH);
    ti->set_valid_epoch(valid_epoch);

    return Status::OK;
    // dtor : release wp_mutex
}

} // namespace shirakami::batch