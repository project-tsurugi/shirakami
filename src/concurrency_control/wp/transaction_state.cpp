
#include "shirakami/interface.h"

namespace shirakami {

Status acquire_tx_state_handle([[maybe_unused]] Token token,
                               [[maybe_unused]] TxStateHandle& handle) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status release_tx_state_handle([[maybe_unused]] TxStateHandle handle) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status tx_check([[maybe_unused]] TxStateHandle handle,
                [[maybe_unused]] TxState& out) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami