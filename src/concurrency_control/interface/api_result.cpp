
#include <memory>

#include "concurrency_control/include/session.h"
#include "database/include/logging.h"

#include "shirakami/api_result.h"

namespace shirakami {

std::shared_ptr<result_info> transaction_result_info(Token token) {
    shirakami_log_entry << "transaction_result_info, token: " << token;
    auto* ti = static_cast<session*>(token);
    shirakami_log_exit << "transaction_result_info";
    return std::make_shared<result_info>(ti->get_result_info());
}

} // namespace shirakami