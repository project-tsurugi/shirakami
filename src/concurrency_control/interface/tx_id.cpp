
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

    std::stringstream ss;
    ss << std::setw(8) << std::setfill('0') << std::hex // NOLINT
       << ti->get_higher_tx_counter();
    tx_id.clear();
    tx_id += ss.str();
    // clear ss
    ss.str("");
    ss.clear(std::stringstream::goodbit);
    ss << std::setw(8) << std::setfill('0') << std::hex << // NOLINT
            ti->get_session_id();
    tx_id += ss.str();
    // clear ss
    ss.str("");
    ss.clear(std::stringstream::goodbit);
    ss << std::setw(16) << std::setfill('0') << std::hex // NOLINT
       << ti->get_tx_counter();
    tx_id += ss.str();

    return Status::OK;
}

} // namespace shirakami