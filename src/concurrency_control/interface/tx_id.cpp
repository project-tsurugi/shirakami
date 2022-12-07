
#include "shirakami/api_tx_id.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

namespace shirakami {

Status get_tx_id(Token token, tx_id& tx_id) {
    // prepare
    auto* ti = static_cast<session*>(token);

    // check tx was begun.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    tx_id.set_higher_info(ti->get_higher_tx_counter());
    tx_id.set_session_id(ti->get_session_id());
    tx_id.set_lower_info(ti->get_tx_counter());

    return Status::OK;
}

} // namespace shirakami