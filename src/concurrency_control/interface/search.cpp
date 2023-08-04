

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

Status exist_key_body(Token const token, Storage const storage, // NOLINT
                      std::string_view const key) {
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    std::string dummy{};
    Status rc{};
    transaction_options::transaction_type this_tx_type{ti->get_tx_type()};
    if (this_tx_type == transaction_options::transaction_type::LONG) {
        rc = long_tx::search_key(ti, storage, key, dummy, false);
    } else if (this_tx_type == transaction_options::transaction_type::SHORT) {
        rc = short_tx::search_key(ti, storage, key, dummy, false);
    } else if (this_tx_type ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::search_key(ti, storage, key, dummy, false);
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    if (rc <= Status::OK) {
        // It is not error by this strand thread, check termination
        std::unique_lock<std::mutex> lk{ti->get_mtx_termination()};
        if (ti->get_result_info().get_reason_code() != reason_code::UNKNOWN) {
            // but concurrent strand thread failed
            if (this_tx_type == transaction_options::transaction_type::LONG) {
                long_tx::abort(ti);
                rc = Status::ERR_CC;
            } else if (this_tx_type ==
                       transaction_options::transaction_type::SHORT) {
                short_tx::abort(ti);
                rc = Status::ERR_CC;
            }
        }
    }
    return rc;
}

Status exist_key(Token const token, Storage const storage, // NOLINT
                 std::string_view const key) {
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = exist_key_body(token, storage, key);
    ti->process_before_finish_step();
    return ret;
}

Status search_key_body(Token const token, Storage const storage, // NOLINT
                       std::string_view const key, std::string& value) {
    // check constraint: key
    auto ret = check_constraint_key_length(key);
    if (ret != Status::OK) { return ret; }

    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    Status rc{};
    transaction_options::transaction_type this_tx_type{ti->get_tx_type()};
    if (this_tx_type == transaction_options::transaction_type::LONG) {
        rc = long_tx::search_key(ti, storage, key, value);
    } else if (this_tx_type == transaction_options::transaction_type::SHORT) {
        rc = short_tx::search_key(ti, storage, key, value);
    } else if (this_tx_type ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::search_key(ti, storage, key, value);
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    if (rc <= Status::OK) {
        // It is not error by this strand thread, check termination
        std::unique_lock<std::mutex> lk{ti->get_mtx_termination()};
        if (ti->get_result_info().get_reason_code() != reason_code::UNKNOWN) {
            // but concurrent strand thread failed
            if (this_tx_type == transaction_options::transaction_type::LONG) {
                long_tx::abort(ti);
                rc = Status::ERR_CC;
            } else if (this_tx_type ==
                       transaction_options::transaction_type::SHORT) {
                short_tx::abort(ti);
                rc = Status::ERR_CC;
            }
        }
    }
    return rc;
}

Status search_key(Token const token, Storage const storage, // NOLINT
                  std::string_view const key, std::string& value) {
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = search_key_body(token, storage, key, value);
    ti->process_before_finish_step();
    return ret;
}

} // namespace shirakami
