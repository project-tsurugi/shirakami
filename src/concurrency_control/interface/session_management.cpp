

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

Status enter_body(Token& token) { // LINT
    Status ret_status = session_table::decide_token(token);
    if (ret_status != Status::OK) return ret_status;

    yakushima::Token kvs_token{};
    while (yakushima::enter(kvs_token) != yakushima::status::OK) {
        _mm_pause();
    }
    static_cast<session*>(token)->set_yakushima_token(kvs_token);

    return Status::OK;
}

Status enter(Token& token) { // LINT
    shirakami_log_entry << "enter, token: " << token;
    auto ret = enter_body(token);
    shirakami_log_exit << "enter, Status: " << ret;
    return ret;
}

void assert_before_unlock(session* const ti) {
    if (ti->get_tx_began()) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "tx began at leave";
    }
    if (ti->get_operating().load(std::memory_order_acquire) != 0) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << "operating is not zero at leave";
    }
}

void unlock_for_other_client(session* const ti) {
    assert_before_unlock(ti);
    ti->set_visible(false); // unlock
}

Status leave_body(Token const token) { // NOLINT
    for (auto&& itr : session_table::get_session_table()) {
        auto* ti = static_cast<session*>(token);
        if (&itr == ti) {
            if (itr.get_visible()) {
                if (itr.get_tx_began()) {
                    // there is a halfway tx.
                    auto rc = shirakami::abort(token);
                    if (rc == Status::WARN_ILLEGAL_OPERATION) {
                        // check truly from ltx
                        if (itr.get_tx_type() !=
                            transaction_options::transaction_type::LONG) {
                            LOG_FIRST_N(ERROR, 1)
                                    << log_location_prefix
                                    << "library programming error";
                        }
                        // the ltx commit was submitted, wait result.
                        do {
                            rc = check_commit(&itr);
                            _mm_pause();
                        } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
                    }
                }

                yakushima::leave(
                        static_cast<session*>(token)->get_yakushima_token());
                unlock_for_other_client(ti);
                return Status::OK;
            }
            return Status::WARN_NOT_IN_A_SESSION;
        }
    }
    return Status::WARN_INVALID_ARGS;
}

Status leave(Token const token) { // NOLINT
    shirakami_log_entry << "leave, token: " << token;
    auto ret = leave_body(token);
    shirakami_log_exit << "leave, Status: " << ret;
    return ret;
}

} // namespace shirakami
