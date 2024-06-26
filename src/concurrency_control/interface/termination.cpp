
#include "concurrency_control/include/session.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"
#include "database/include/logging.h"

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
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return rc;
    }
    return rc;
}

Status abort(Token token) { // NOLINT
    shirakami_log_entry << "abort, token: " << token;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::lock_guard<std::shared_mutex> lock{ti->get_mtx_state_da_term()};

        // abort_body check warn not begin
        ret = abort_body(token);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "abort, Status: " << ret;
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
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << "library programming error";
        return Status::ERR_FATAL;
    }

    return rc;
}

Status commit(Token const token) { // NOLINT
    shirakami_log_entry << "commit, token: " << token;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::lock_guard<std::shared_mutex> lock{ti->get_mtx_state_da_term()};

        // commit_body check warn not begin
        ret = commit_body(token);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "commit, Status: " << ret;
    return ret;
}

bool commit(Token token, commit_callback_type callback) { // NOLINT
    shirakami_log_entry << "commit, token: " << token;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::lock_guard<std::shared_mutex> lock{ti->get_mtx_state_da_term()};

        // commit_body check warn not begin
        ret = commit_body(token, std::move(callback));
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "commit, bool: "
                       << (ret != Status::WARN_WAITING_FOR_OTHER_TX);
    return ret != Status::WARN_WAITING_FOR_OTHER_TX;
}

Status check_commit(Token token) {
    // check commit is for only ltx
    shirakami_log_entry << "check_commit, token: " << token;
    auto ret = long_tx::check_commit(token);
    shirakami_log_exit << "check_commit, Status: " << ret;
    return ret;
}

} // namespace shirakami
