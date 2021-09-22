
#include "concurrency_control/wp/include/session.h"

#include "shirakami/interface.h"

namespace shirakami {

Status enter(Token& token) { // NOLINT
    Status ret_status = session_table::decide_token(token);
    if (ret_status != Status::OK) return ret_status;

    yakushima::Token kvs_token{};
    while (yakushima::enter(kvs_token) != yakushima::status::OK) {
        _mm_pause();
    }
    static_cast<session*>(token)->set_yakushima_token(kvs_token);

    return Status::OK;
}

void fin([[maybe_unused]] bool force_shut_down_cpr) try {
} catch (std::exception& e) {
    std::cerr << "fin() : " << e.what() << std::endl;
}

Status init([[maybe_unused]] bool enable_recovery, [[maybe_unused]] const std::string_view log_directory_path) { // NOLINT
    // about cc
    session_table::init_session_table(enable_recovery);
    //epoch::invoke_epoch_thread();

    // about index
    yakushima::init();

    return Status::OK;
}

Status leave([[maybe_unused]] Token const token) { // NOLINT
    return Status::OK;
}

void tx_begin([[maybe_unused]] Token const token, [[maybe_unused]] bool const read_only, [[maybe_unused]] bool const for_batch) { // NOLINT
}

} // namespace shirakami