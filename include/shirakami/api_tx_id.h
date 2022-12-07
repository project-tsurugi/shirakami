#pragma once

#include "scheme.h"
#include "tx_id.h"

namespace shirakami {

/**
 * @brief Get the tx id object.
 * @param[in] token the transaction handle.
 * @param[out] tx_id output argument.
 * @return Status::OK success.
 * @return Status::WARN_NOT_BEGIN The transaction is not begun.
 * @note If the token is invalid, this cause undefined behavior.
 */
Status get_tx_id(Token token, tx_id& tx_id);

} // namespace shirakami