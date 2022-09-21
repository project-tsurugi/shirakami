
#include <memory>

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/api_result.h"

namespace shirakami {

std::shared_ptr<result_info> transaction_result_info(Token token) {
    auto* ti = static_cast<session*>(token);
    return std::make_shared<result_info>(ti->get_result_info());
}

} // namespace shirakami