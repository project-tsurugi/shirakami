
#include "include/session.h"
#include "include/tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami {

Status session_table::decide_token(Token& token) { // NOLINT
    for (auto&& itr : get_session_table()) {
        if (!itr.get_visible()) {
            bool expected(false);
            bool desired(true);
            if (itr.cas_visible(expected, desired)) {
                token = static_cast<void*>(&itr);
                break;
            }
        }
        if (&itr == get_session_table().end() - 1) {
            return Status::ERR_SESSION_LIMIT;
        }
    }

    return Status::OK;
}

void session_table::init_session_table() {
    std::size_t worker_number = 0;
    for (auto&& itr : get_session_table()) {
        // for external
        itr.set_visible(false);
        // for internal
        itr.clean_up();
        // clear metadata about auto commit.
        itr.set_requested_commit(false);
        // for tx counter
        itr.set_higher_tx_counter(0);
        itr.set_tx_counter(0);
        itr.set_session_id(worker_number);
#ifdef PWAL
        itr.get_lpwal_handle().init();
        itr.get_lpwal_handle().set_worker_number(worker_number);
#endif
        ++worker_number;
    }
}

} // namespace shirakami