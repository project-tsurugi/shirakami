#pragma once

#include "scheme.h"
#include "tx_id.h"

namespace shirakami {

/**
 * @brief Get the tx id object.
 * @param[in] token the transaction handle.
 * @param[out] tx_id output argument. This string is represented by 32 
 * hexadecimal digits.
 * @return Status::OK success.
 * @return Status::WARN_NOT_BEGIN The transaction is not begun.
 * @note If the token is invalid, this cause undefined behavior.
 */
Status get_tx_id(Token token, std::string& tx_id);

/**
 * @brief Get the tx id object.
 * @param[in] token the transaction handle.
 * @param[out] tx_id output argument. This is composed TID- + 16 hexadecimal 
 * digits, lowercase English by string information.
 * @return Status::OK success.
 * @return Status::WARN_NOT_BEGIN The transaction is not begun.
 * @note If the token is invalid, this cause undefined behavior.
 */
Status get_tx_id_for_uid(Token token, std::string& tx_id);

} // namespace shirakami