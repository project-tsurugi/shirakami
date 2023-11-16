
#include <sstream>

#include "shirakami/api_tx_id.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "database/include/logging.h"

namespace shirakami {

Status get_tx_id_body(Token token, std::string& tx_id) {
    // prepare
    auto* ti = static_cast<session*>(token);

    // check tx was begun.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    tx_id.clear();
    tx_id += "TID-";

    // prepare string stream for higher tx counter
    std::stringstream ss;
    ss << std::setw(4) << std::setfill('0') << std::hex // NOLINT
       << static_cast<std::uint16_t>(ti->get_higher_tx_counter());
    // use down cast
    // add to tx id
    tx_id += ss.str();
    // clear ss
    ss.str("");

    // prepare string stream for session id
    ss.clear(std::stringstream::goodbit);
    ss << std::setw(4) << std::setfill('0') << std::hex << // NOLINT
            static_cast<std::uint16_t>(ti->get_session_id());
    // use down cast
    // add to tx id
    tx_id += ss.str();
    // clear ss
    ss.str("");

    // prepare string stream for tx counter
    ss.clear(std::stringstream::goodbit);
    ss << std::setw(8) << std::setfill('0') << std::hex // NOLINT
       << static_cast<std::uint32_t>(ti->get_tx_counter());
    // use down cast
    tx_id += ss.str();

    return Status::OK;
}

Status get_tx_id(Token token, std::string& tx_id) {
    shirakami_log_entry << "get_tx_id, token: " << token
                        << ", tx_id: " << tx_id;
    auto ret = get_tx_id_body(token, tx_id);
    shirakami_log_exit << "get_tx_id, Status: " << ret << ", tx_id: " << tx_id;
    return ret;
}

} // namespace shirakami