
#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "database/include/logging.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status acquire_tx_state_handle_body(Token const token, // NOLINT
                                    TxStateHandle& handle) {
    auto* ti{static_cast<session*>(token)};
    // check whether it already begun.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    // check whether it already get tx state.
    if (ti->get_has_current_tx_state_handle()) {
        handle = ti->get_current_tx_state_handle();
        return Status::WARN_ALREADY_EXISTS;
    }

    // generate tx state handle
    ti->set_has_current_tx_state_handle(true);
    TxStateHandle hd{TxState::get_new_handle_ctr()};
    ti->set_current_tx_state_handle(hd);
    handle = hd;
    TxState::insert_tx_state(hd);
    TxState& ts{TxState::get_tx_state(hd)};
    ti->set_current_tx_state_ptr(&ts);
    ts.set_token(token);
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        ts.set_serial_epoch(0);
        ts.set_kind(TxState::StateKind::STARTED);
    } else if (ti->get_tx_type() ==
                       transaction_options::transaction_type::LONG ||
               ti->get_tx_type() ==
                       transaction_options::transaction_type::READ_ONLY) {
        ts.set_serial_epoch(static_cast<std::uint64_t>(ti->get_valid_epoch()));
        if (
                // wait staging
                ti->get_valid_epoch() > epoch::get_global_epoch() ||
                // wait high priori short
                ti->find_high_priority_short() == Status::WARN_PREMATURE) {
            ts.set_kind(TxState::StateKind::WAITING_START);
        } else {
            ts.set_kind(TxState::StateKind::STARTED);
        }
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status acquire_tx_state_handle(Token const token, // NOLINT
                               TxStateHandle& handle) {
    shirakami_log_entry << "acquire_tx_state_handle, token: " << token
                        << ", handle: " << handle;
    auto ret = acquire_tx_state_handle_body(token, handle);
    shirakami_log_exit << "acquire_tx_state_handle, Status: " << ret;
    return ret;
}

Status release_tx_state_handle_body(TxStateHandle handle) {
    if (handle == undefined_handle) { return Status::WARN_INVALID_HANDLE; }
    Token token{};
    auto rc{TxState::find_and_erase_tx_state(handle, token)};
    if (rc == Status::OK) {
        auto* ti{static_cast<session*>(token)};
        if (handle == ti->get_current_tx_state_handle()) {
            // the tx is running.
            ti->clear_about_tx_state();
        }
        return Status::OK;
    }
    return rc;
}

Status release_tx_state_handle(TxStateHandle handle) {
    shirakami_log_entry << "release_tx_state_handle, handle: " << handle;
    auto ret = release_tx_state_handle_body(handle);
    shirakami_log_exit << "release_tx_state_handle, Status: " << ret;
    return ret;
}

Status check_tx_state_body(TxStateHandle handle, TxState& out) {
    if (handle == undefined_handle) { return Status::WARN_INVALID_HANDLE; }
    auto rc{TxState::find_and_get_tx_state(handle, out)};
    if (rc == Status::WARN_INVALID_HANDLE) { return rc; }
    // handle is valid
    auto& ts{TxState::get_tx_state(handle)};
    // short tx
    if (out.get_serial_epoch() == 0) {
        if (ts.state_kind() == TxState::StateKind::WAITING_DURABLE) {
#ifdef PWAL
            if (ts.get_durable_epoch() <=
                epoch::get_datastore_durable_epoch()) {
                ts.set_kind(TxState::StateKind::DURABLE);  // for internal
                out.set_kind(TxState::StateKind::DURABLE); // for external
            }
#else
            // if no logging, it must not be waiting_durable status
            LOG(ERROR) << log_location_prefix << "unexpected path";
            return Status::ERR_FATAL;
#endif
        }
        return Status::OK;
    }

    // long tx
    if (out.get_serial_epoch() <= epoch::get_global_epoch()) {
        if (ts.state_kind() == TxState::StateKind::WAITING_START) {
            // check high priori stx
            auto* ti = static_cast<session*>(ts.get_token());
            if (ti->find_high_priority_short() != Status::WARN_PREMATURE) {
                ts.set_kind(TxState::StateKind::STARTED);  // for internal
                out.set_kind(TxState::StateKind::STARTED); // for external
            }                                              // else : premature
        } else if (ts.state_kind() == TxState::StateKind::WAITING_CC_COMMIT) {
            // waiting for commit by background worker.
            out.set_kind(TxState::StateKind::WAITING_CC_COMMIT); // for external
        } else if (ts.state_kind() == TxState::StateKind::WAITING_DURABLE) {
#ifdef PWAL
            if (ts.get_durable_epoch() <=
                epoch::get_datastore_durable_epoch()) {
                ts.set_kind(TxState::StateKind::DURABLE);  // for internal
                out.set_kind(TxState::StateKind::DURABLE); // for external
            }
#else
            // if no logging, it must not be waiting_durable status
            LOG(ERROR) << log_location_prefix << "unexpected path";
            return Status::ERR_FATAL;
#endif
        }
    } else {
        ts.set_kind(TxState::StateKind::WAITING_START);  // for internal
        out.set_kind(TxState::StateKind::WAITING_START); // for external
    }


    return Status::OK;
}

Status check_tx_state(TxStateHandle handle, TxState& out) {
    shirakami_log_entry << "check_tx_state, handle: " << handle
                        << ", out: " << out;
    auto ret = check_tx_state_body(handle, out);
    shirakami_log_exit << "check_tx_state, Status: " << ret << ", out: " << out;
    return ret;
}

Status check_ltx_is_highest_priority_body(Token token, bool& out) {
    // check the tx is already began.
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    // the tx was already began.

    // check the tx is ltx mode
    if (ti->get_tx_type() != transaction_options::transaction_type::LONG) {
        return Status::WARN_INVALID_ARGS;
    }

    {
        // take shared lock for ongoing tx info
        std::lock_guard<std::shared_mutex> lk{ongoing_tx::get_mtx()};
        // check highest tx id
        std::size_t highest_tx_id{std::get<ongoing_tx::index_id>(
                (*ongoing_tx::get_tx_info().begin()))};
        out = highest_tx_id == ti->get_long_tx_id();
    }

    return Status::OK;
}

Status check_ltx_is_highest_priority(Token token, bool& out) {
    shirakami_log_entry << "check_ltx_is_highest_priority, token: " << token
                        << ", out: " << out;
    auto ret = check_ltx_is_highest_priority_body(token, out);
    shirakami_log_exit << "check_ltx_is_highest_priority, Status: " << ret;
    return ret;
}

} // namespace shirakami