#pragma once

#include <memory>

#include "result_info.h"
#include "scheme.h"

namespace shirakami {

/**
 * @brief get transaction result information after the transaction was aborted.
 * @param[in] token transaction handle.
 * @return std::shared_ptr<result_info> 
 */
std::shared_ptr<result_info> transaction_result_info(Token token);

} // namespace shirakami