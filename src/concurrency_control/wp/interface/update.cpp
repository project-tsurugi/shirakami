
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "shirakami/interface.h"

namespace shirakami {

Status update([[maybe_unused]] Token token, [[maybe_unused]] Storage storage,
              [[maybe_unused]] const std::string_view key, // NOLINT
              [[maybe_unused]] const std::string_view val) {
    auto* ti = static_cast<session*>(token);

    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    //update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    if (ti->get_mode() == tx_mode::BATCH) {
        return batch::upsert(ti, storage, key, val);
    }
    if (ti->get_mode() == tx_mode::OCC) {
        return occ::upsert(ti, storage, key, val);
    }
    LOG(FATAL);

    return Status::ERR_FATAL;
}

} // namespace shirakami
