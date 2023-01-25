

#include <string_view>

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status exist_key(Token const token, Storage const storage,
                 std::string_view const key) {
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    std::string dummy{};
    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        rc = long_tx::search_key(ti, storage, key, dummy, false);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::SHORT) {
        rc = short_tx::search_key(ti, storage, key, dummy, false);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::search_key(ti, storage, key, dummy, false);
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    ti->process_before_finish_step();
    return rc;
}

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, std::string& value) {
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        rc = long_tx::search_key(ti, storage, key, value);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::SHORT) {
        rc = short_tx::search_key(ti, storage, key, value);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::search_key(ti, storage, key, value);
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    ti->process_before_finish_step();
    return rc;
}

} // namespace shirakami
