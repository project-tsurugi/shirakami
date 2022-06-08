
#include "include/session.h"
#include "include/tuple_local.h"

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

void session_table::init_session_table([[maybe_unused]] bool enable_recovery) {
    for (auto&& itr : get_session_table()) {
        // for external
        itr.set_visible(false);
        // for internal
        itr.clean_up();
    }
}

void session_table::fin_session_table() {
    std::vector<std::thread> th_vc;
    th_vc.reserve(get_session_table().size());
    for (auto&& itr : get_session_table()) {
        auto process = [&itr]() { itr.clear_local_set(); };
        th_vc.emplace_back(process);
    }

    for (auto&& th : th_vc) th.join();
}

} // namespace shirakami