
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status abort(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    ti->process_before_start_step();

    // set result info
    ti->set_result(reason_code::USER_ABORT);

    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        rc = short_tx::abort(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        if (ti->get_requested_commit()) {
            /**
             * It was already requested.
             * So user must use check_commit function to check result.
             */
            ti->process_before_finish_step();
            return Status::WARN_ILLEGAL_OPERATION;
        }
        rc = long_tx::abort(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::abort(ti);
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return rc;
    }
    ti->process_before_finish_step();
    return rc;
}

Status commit(Token const token) {
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    ti->process_before_start_step();

    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        rc = short_tx::commit(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        if (ti->get_requested_commit()) {
            /**
             * It was already requested.
             * So user must use check_commit function to check result.
             */
            return Status::WARN_WAITING_FOR_OTHER_TX;
        }
        rc = long_tx::commit(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::commit(ti);
    } else {
        LOG(ERROR) << log_location_prefix << "unexpected error";
        return Status::ERR_FATAL;
    }
    ti->process_before_finish_step();
    return rc;
}

Status check_commit(Token token) {
    // check commit is for only ltx
    return long_tx::check_commit(token);
}

} // namespace shirakami