
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status acquire_tx_state_handle(Token const token, TxStateHandle& handle) {
    auto* ti{static_cast<session*>(token)};
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    if (ti->get_has_current_tx_state_handle()) {
        handle = ti->get_current_tx_state_handle();
        return Status::WARN_ALREADY_EXISTS;
    }

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
        if (ti->get_valid_epoch() > epoch::get_global_epoch()) {
            ts.set_kind(TxState::StateKind::WAITING_START);
        } else {
            ts.set_kind(TxState::StateKind::STARTED);
        }
    } else {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status release_tx_state_handle(TxStateHandle handle) {
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

Status tx_check(TxStateHandle handle, TxState& out) {
    if (handle == undefined_handle) { return Status::WARN_INVALID_HANDLE; }
    auto rc{TxState::find_and_get_tx_state(handle, out)};
    if (rc == Status::WARN_INVALID_HANDLE) { return rc; }
    auto& ts{TxState::get_tx_state(handle)};
    if (out.get_serial_epoch() == 0) {
        // short tx
        if (ts.state_kind() == TxState::StateKind::WAITING_DURABLE) {
#ifdef PWAL
            if (ts.get_durable_epoch() <= lpwal::get_durable_epoch()) {
                ts.set_kind(TxState::StateKind::DURABLE);  // for internal
                out.set_kind(TxState::StateKind::DURABLE); // for external
            }
#else
            // if no logging, it must not be waiting_durable status
            LOG(ERROR) << "unexpected path";
            return Status::ERR_FATAL;
#endif
        }
        return Status::OK;
    }

    // long tx
    if (out.get_serial_epoch() <= epoch::get_global_epoch()) {
        if (ts.state_kind() == TxState::StateKind::WAITING_START) {
            ts.set_kind(TxState::StateKind::STARTED);  // for internal
            out.set_kind(TxState::StateKind::STARTED); // for external
        } else if (ts.state_kind() == TxState::StateKind::WAITING_CC_COMMIT) {
            // waiting for commit by background worker.
            out.set_kind(TxState::StateKind::WAITING_CC_COMMIT); // for external
        } else if (ts.state_kind() == TxState::StateKind::WAITING_DURABLE) {
#ifdef PWAL
            if (ts.get_durable_epoch() <= lpwal::get_durable_epoch()) {
                ts.set_kind(TxState::StateKind::DURABLE);  // for internal
                out.set_kind(TxState::StateKind::DURABLE); // for external
            }
#else
            // if no logging, it must not be waiting_durable status
            LOG(ERROR) << "unexpected path";
            return Status::ERR_FATAL;
#endif
        }
    } else {
        ts.set_kind(TxState::StateKind::WAITING_START);  // for internal
        out.set_kind(TxState::StateKind::WAITING_START); // for external
    }


    return Status::OK;
}

} // namespace shirakami