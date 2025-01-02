

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"
#include "database/include/logging.h"
#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

static Status tx_begin_body(transaction_options options) { // NOLINT
    // get tx options
    Token token = options.get_token();

    // get thread info
    auto* ti = static_cast<session*>(token);
    if (ti->get_tx_began()) { return Status::WARN_ALREADY_BEGIN; }

    // clear abort result info
    {
        std::unique_lock<std::mutex> lk{ti->get_mtx_result_info()};
        ti->get_result_info().clear();
    }

    // this tx is not began.
    transaction_options::transaction_type tx_type =
            options.get_transaction_type();
    transaction_options::write_preserve_type write_preserve =
            options.get_write_preserve();
    if (!write_preserve.empty()) {
        if (tx_type != transaction_options::transaction_type::LONG) {
            // The only ltx can use write preserve.
            return Status::WARN_ILLEGAL_OPERATION;
        }
    }
    if (tx_type == transaction_options::transaction_type::LONG) {
        return Status::ERR_NOT_IMPLEMENTED;
        // NOLINTNEXTLINE(*-else-after-return)
    } else if (tx_type == transaction_options::transaction_type::SHORT) {
        ti->init_flags_for_stx_begin();
    } else if (tx_type == transaction_options::transaction_type::READ_ONLY) {
        ti->init_flags_for_rtx_begin();
        auto rc{read_only_tx::tx_begin(ti)};
        if (rc != Status::OK) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix << rc << ", unreachable path";
            return rc;
        }
    } else {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    // begin success, set begin epoch
    ti->set_begin_epoch(epoch::get_global_epoch());

    // about tx counter
    if (tx_id::is_max_lower_info(ti->get_tx_counter())) {
        ti->set_higher_tx_counter(ti->get_higher_tx_counter() + 1);
        ti->set_tx_counter(0);
    } else {
        ti->set_tx_counter(ti->get_tx_counter() + 1);
    }

    // success tx begin
    // process about diagnostics
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        ti->set_diag_tx_state_kind(TxState::StateKind::STARTED);
    } else {
        // ltx and rtx
        ti->set_diag_tx_state_kind(TxState::StateKind::WAITING_START);
    }

    // select required mutex
    auto& mflags = ti->get_mutex_flags();
    mflags.set_readaccess_daterm(
            (tx_type != transaction_options::transaction_type::READ_ONLY) // if not RTX -> true
            || session::optflag_rtx_da_term_mutex); // if RTX -> optflag_rtx_da_term_mutex

    /**
     * This is for concurrent programming. It teaches to other thread that this
     * tx began at last.
     */
    ti->set_tx_began(true);
    return Status::OK;
}

Status tx_begin(transaction_options options) { // NOLINT
    shirakami_log_entry << "tx_begin, options: " << options;
    Token token = options.get_token();
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    auto ret = tx_begin_body(options);
    ti->process_before_finish_step();
    shirakami_log_exit << "tx_begin, Status: " << ret;
    return ret;
}

} // namespace shirakami
