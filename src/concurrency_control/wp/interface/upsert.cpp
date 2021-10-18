
#include "concurrency_control/wp/include/session.h"

#include "shirakami/interface.h"

namespace shirakami {

Status upsert(Token token, [[maybe_unused]] Storage storage,
              [[maybe_unused]] const std::string_view key, // NOLINT
              [[maybe_unused]] const std::string_view val) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    if (ti->get_read_only()) { return Status::WARN_INVALID_HANDLE; }
    if (ti->get_mode() == tx_mode::BATCH) {
        if (!ti->check_exist_wp_set(storage)) {
            return Status::WARN_INVALID_ARGS;
        }
    }

    return Status::OK;
}

} // namespace shirakami
