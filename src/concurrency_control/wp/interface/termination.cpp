
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/short_tx/include/short_tx.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status abort(Token token) { // NOLINT
    // clean up local set
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    } else {
        //update metadata
        ti->set_step_epoch(epoch::get_global_epoch());
    }

    if (ti->get_tx_type() == TX_TYPE::LONG) { return long_tx::abort(ti); }
    return short_tx::abort(ti);
}

Status commit([[maybe_unused]] Token token, // NOLINT
              [[maybe_unused]] commit_param* cp) {
    auto* ti = static_cast<session*>(token);
    // check whether it already began.
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    } else {
        //update metadata
        ti->set_step_epoch(epoch::get_global_epoch());
    }

    if (ti->get_tx_type() == TX_TYPE::LONG) { return long_tx::commit(ti, cp); }
    return short_tx::commit(ti, cp);
}

bool check_commit([[maybe_unused]] Token token, // NOLINT
                  [[maybe_unused]] std::uint64_t commit_id) {
    // todo
    // ERR_NOT_IMPLEMENTED
    return true;
}

} // namespace shirakami
