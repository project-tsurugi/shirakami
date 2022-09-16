#pragma once

#include <memory>

#include "result_info.h"
#include "scheme.h"

namespace shirakami {

std::shared_ptr<result_info> transaction_result_info(Token token);

} // namespace shirakami