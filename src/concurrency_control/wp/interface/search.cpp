
#include "concurrency_control/wp/include/session.h"

#include "shirakami/interface.h"

namespace shirakami {

Status search_key(Token token, [[maybe_unused]] Storage storage,
                  [[maybe_unused]] const std::string_view key, // NOLINT
                  [[maybe_unused]] Tuple** const tuple) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    // index access
    // version selection

    // occ
    // wp check
    // read version

    // batch
    // read version

    return Status::OK;
}

} // namespace shirakami
