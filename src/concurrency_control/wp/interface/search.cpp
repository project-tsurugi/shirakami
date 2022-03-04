

#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/batch/include/long_tx.h"
#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status exist_key(Token const token, Storage const storage,
                 std::string_view const key) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    std::string dummy{};
    if (ti->get_tx_type() == TX_TYPE::LONG) {
        return long_tx::search_key(ti, storage, key, dummy, false);
    }
    if (ti->get_tx_type() == TX_TYPE::SHORT) {
        return occ::search_key(ti, storage, key, dummy, false);
    }
    LOG(FATAL) << "unreachable";
    return Status::ERR_FATAL;
}

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, std::string& value) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }

    // update metadata
    ti->set_step_epoch(epoch::get_global_epoch());

    if (ti->get_tx_type() == TX_TYPE::LONG) {
        return long_tx::search_key(ti, storage, key, value);
    }
    if (ti->get_tx_type() == TX_TYPE::SHORT) {
        return occ::search_key(ti, storage, key, value);
    }
    LOG(FATAL) << "unreachable";
    return Status::ERR_FATAL;
}

} // namespace shirakami
