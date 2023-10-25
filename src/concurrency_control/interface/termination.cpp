
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status abort_body(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    // set result info
    ti->set_result(reason_code::USER_ABORT);

    // set about diagnostics
    ti->set_diag_tx_state_kind(TxState::StateKind::ABORTED);

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
    return rc;
}

Status abort(Token token) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = abort_body(token);
    ti->process_before_finish_step();
    return ret;
}

Status commit_body(Token const token,                    // NOLINT
                   commit_callback_type callback = {}) { // NOLINT
    // default 引数は後方互換性のため。いずれ削除する。
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) {
        if (callback) { callback(Status::WARN_NOT_BEGIN, {}, 0); }
        return Status::WARN_NOT_BEGIN;
    }

    // log callback
    ti->set_commit_callback(callback); // NOLINT

    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        // for short tx
        rc = short_tx::commit(ti);

        // set about diagnostics
        if (rc == Status::OK) {
            ti->set_diag_tx_state_kind(TxState::StateKind::WAITING_DURABLE);
        } else {
            ti->set_diag_tx_state_kind(TxState::StateKind::ABORTED);
        }
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        // for long tx
        if (ti->get_requested_commit()) {
            /**
             * It was already requested.
             * So user must use check_commit function to check result.
             */
            return Status::WARN_WAITING_FOR_OTHER_TX;
        }
        rc = long_tx::commit(ti);

        // set about diagnostics
        if (rc == Status::OK) {
            // committed
            ti->set_diag_tx_state_kind(TxState::StateKind::WAITING_DURABLE);
        } else if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
            // waited
            ti->set_diag_tx_state_kind(TxState::StateKind::WAITING_CC_COMMIT);
        } else {
            // aborted
            ti->set_diag_tx_state_kind(TxState::StateKind::ABORTED);
        }
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        // for read only tx
        rc = read_only_tx::commit(ti);
        // set about diagnostics. it must commit
        ti->set_diag_tx_state_kind(TxState::StateKind::WAITING_DURABLE);
    } else {
        LOG(ERROR) << log_location_prefix << "unexpected error";
        return Status::ERR_FATAL;
    }

    return rc;
}

Status commit(Token const token) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = commit_body(token);
    ti->process_before_finish_step();
    return ret;
}

bool commit(Token token, commit_callback_type callback) { // NOLINT
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = commit_body(token, std::move(callback));
    ti->process_before_finish_step();
    return ret != Status::WARN_WAITING_FOR_OTHER_TX;
}

Status check_commit(Token token) {
    // check commit is for only ltx
    return long_tx::check_commit(token);
}

} // namespace shirakami