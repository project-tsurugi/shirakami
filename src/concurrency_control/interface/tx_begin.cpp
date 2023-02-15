

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#ifdef PWAL

#include "concurrency_control/include/lpwal.h"

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#include "limestone/api/datastore.h"

#endif

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "boost/filesystem/path.hpp"

#include "glog/logging.h"

namespace shirakami {

Status tx_begin(transaction_options options) { // NOLINT
                                               // get tx options
    Token token = options.get_token();

    // get thread info
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    if (ti->get_tx_began()) {
        ti->process_before_finish_step();
        return Status::WARN_ALREADY_BEGIN;
    }
    // clear abort reason
    ti->get_result_info().set_reason_code(reason_code::UNKNOWN);

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
        /**
             * It may be called without check_commit for the ltx.
             * Clear metadata initialized at check_commit.
             */
        ti->set_requested_commit(false);

        auto rc{long_tx::tx_begin(ti, write_preserve, options.get_read_area())};
        if (rc != Status::OK) {
            ti->process_before_finish_step();
            return rc;
        }
        ti->get_write_set().set_for_batch(true);
    } else if (tx_type == transaction_options::transaction_type::SHORT) {
        ti->get_write_set().set_for_batch(false);
    } else if (tx_type == transaction_options::transaction_type::READ_ONLY) {
        auto rc{read_only_tx::tx_begin(ti)};
        if (rc != Status::OK) {
            LOG(ERROR) << log_location_prefix << rc << ", unreachable path";
            ti->process_before_finish_step();
            return rc;
        }
    } else {
        LOG(ERROR) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }
    ti->set_tx_type(tx_type);
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
    ti->process_before_finish_step();

    /**
     * This is for concurrent programming. It teaches to other thread that this
     * tx began at last.
     */
    ti->set_tx_began(true);
    return Status::OK;
}

} // namespace shirakami