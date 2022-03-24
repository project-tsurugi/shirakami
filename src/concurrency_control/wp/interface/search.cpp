

#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/short_tx/include/short_tx.h"

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
    ti->process_before_start_step();

    std::string dummy{};
    Status rc{};
    if (ti->get_tx_type() == TX_TYPE::LONG) {
        rc = long_tx::search_key(ti, storage, key, dummy, false);
    } else if (ti->get_tx_type() == TX_TYPE::SHORT) {
        rc = short_tx::search_key(ti, storage, key, dummy, false);
    } else {
        LOG(FATAL) << "unreachable";
        return Status::ERR_FATAL;
    }
    ti->process_before_finish_step();
    return rc;
}

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, std::string& value) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    ti->process_before_start_step();

    Status rc{};
    if (ti->get_tx_type() == TX_TYPE::LONG) {
        rc = long_tx::search_key(ti, storage, key, value);
    } else if (ti->get_tx_type() == TX_TYPE::SHORT) {
        rc = short_tx::search_key(ti, storage, key, value);
    } else {
        LOG(FATAL) << "unreachable";
        return Status::ERR_FATAL;
    }
    ti->process_before_finish_step();
    return rc;
}

} // namespace shirakami
