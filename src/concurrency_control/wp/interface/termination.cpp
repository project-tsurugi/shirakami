
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
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    Status rc{};
    if (ti->get_tx_type() == TX_TYPE::SHORT) {
        rc = short_tx::abort(ti);
    } else if (ti->get_tx_type() == TX_TYPE::LONG) {
        rc = long_tx::abort(ti);
    } else if (ti->get_tx_type() == TX_TYPE::READ_ONLY) {
        rc = read_only_tx::abort(ti);
    } else {
        LOG(ERROR) << "programming error";
        return rc;
    }
    ti->process_before_finish_step();
    return rc;
}

Status commit([[maybe_unused]] Token token, // NOLINT
              [[maybe_unused]] commit_param* cp) {
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    Status rc{};
    if (ti->get_tx_type() == TX_TYPE::SHORT) {
        rc = short_tx::commit(ti, cp);
    } else if (ti->get_tx_type() == TX_TYPE::LONG) {
        rc = long_tx::commit(ti, cp);
    } else if (ti->get_tx_type() == TX_TYPE::READ_ONLY) {
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
