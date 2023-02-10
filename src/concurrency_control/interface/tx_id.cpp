
#include <sstream>

#include "shirakami/api_tx_id.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

namespace shirakami {

Status get_tx_id(Token token, std::string& tx_id) {
    // prepare
    auto* ti = static_cast<session*>(token);

    // check tx was begun.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    tx_id.clear();
    tx_id += "TID-";

    // prepare string stream for higher tx counter
    std::stringstream ss;
    ss << std::setw(8) << std::setfill('0') << std::hex // NOLINT
       << ti->get_higher_tx_counter();
    // add to tx id
    tx_id += ss.str();
    // clear ss
    ss.str("");

    // prepare string stream for session id
    ss.clear(std::stringstream::goodbit);
    ss << std::setw(8) << std::setfill('0') << std::hex << // NOLINT
            ti->get_session_id();
    // add to tx id
    tx_id += ss.str();
    // clear ss
    ss.str("");

    // prepare string stream for tx counter
    ss.clear(std::stringstream::goodbit);
    ss << std::setw(16) << std::setfill('0') << std::hex // NOLINT
       << ti->get_tx_counter();
    tx_id += ss.str();

    return Status::OK;
}

} // namespace shirakami