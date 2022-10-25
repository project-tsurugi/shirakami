
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/read_only_tx/include/read_only_tx.h"
#include "concurrency_control/wp/interface/short_tx/include/short_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status abort(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    // set result info
    ti->set_result(reason_code::USER_ABORT);

    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        rc = short_tx::abort(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        rc = long_tx::abort(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::abort(ti);
    } else {
        LOG(ERROR) << "programming error";
        return rc;
    }
    ti->process_before_finish_step();
    return rc;
}

Status commit(Token const token) {
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin({token}); // NOLINT
    }
    ti->process_before_start_step();

    Status rc{};
    if (ti->get_tx_type() == transaction_options::transaction_type::SHORT) {
        rc = short_tx::commit(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::LONG) {
        rc = long_tx::commit(ti);
    } else if (ti->get_tx_type() ==
               transaction_options::transaction_type::READ_ONLY) {
        rc = read_only_tx::commit(ti);
    } else {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }
    ti->process_before_finish_step();
    return rc;
}

bool check_commit([[maybe_unused]] Token token, // NOLINT
                  [[maybe_unused]] std::uint64_t commit_id) {
    // todo
    // ERR_NOT_IMPLEMENTED
    return true;
}

} // namespace shirakami