#pragma once

#include <memory>

#include "result_info.h"
#include "scheme.h"

namespace shirakami {

/**
 * @brief get transaction result information after the transaction was aborted.
 * @details When this function is called after a transaction has been aborted, 
 * the information about the abort contained in the Token is copied to the heap 
 * memory object and passed. Therefore, the information returned is available 
 * and unchanged until the user discards it.
 * @param[in] token transaction handle.
 * @return std::shared_ptr<result_info> 
 */
std::shared_ptr<result_info> transaction_result_info(Token token);

} // namespace shirakami