#pragma once

#include <memory>

#include "result_info.h"
#include "scheme.h"

namespace shirakami {

/**
 * @brief get transaction result information after the transaction was aborted.
 * @details This function is available after the transaction is aborted until
 * the tx_begin, leave function is called. When this function is called after a
 * transaction has been aborted, the information about the abort contained in
 * the Token is copied to the heap memory object and passed.
 * @param[in] token transaction handle.
 * @pre This is updated concurrently by strand thread, so you must not check this
 * untill you check termination.
 * @return std::shared_ptr<result_info>
 */
std::shared_ptr<result_info> transaction_result_info(Token token);

} // namespace shirakami
