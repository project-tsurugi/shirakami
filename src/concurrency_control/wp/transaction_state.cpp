
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami {

Status acquire_tx_state_handle([[maybe_unused]] Token token,
                               [[maybe_unused]] TxStateHandle& handle) {
    auto* ti{static_cast<session*>(token)};
    if (!ti->get_tx_began()) {
        return Status::WARN_NOT_BEGIN;
    }

    if (ti->get_has_current_tx_state_handle()) {
        handle = ti->get_current_tx_state_handle();
        return Status::WARN_ALREADY_EXISTS;
    }
    ti->set_has_current_tx_state_handle(true);

    return Status::OK;
}

Status release_tx_state_handle([[maybe_unused]] TxStateHandle handle) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status tx_check([[maybe_unused]] TxStateHandle handle,
                [[maybe_unused]] TxState& out) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami