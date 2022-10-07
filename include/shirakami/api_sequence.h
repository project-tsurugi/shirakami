#pragma once

#include "scheme.h"

namespace shirakami {

/**
 * About sequence function
 */

/**
 * @brief sequence id
 * @details the identifier that uniquely identifies the sequence in the database
 */
using SequenceId = std::size_t;

/**
 * @brief sequence value
 * @details the value of the sequence. Each value in the sequence is associated 
 * with some version number.
 */
using SequenceValue = std::int64_t;

/**
 * @brief sequence version
 * @details the version number of the sequence that begins at 0 and increases 
 * monotonically. For each version in the sequence, there is the associated 
 * value with it.
 */
using SequenceVersion = std::size_t;

/**
 * @brief create new sequence
 * @param [out] id the newly assigned sequence id, that is valid only 
 * when this function is successful with Status::OK.
 * @param[in] token If this is not nullptr, shirakami uses @a token for logging.
 * @return Status::OK success.
 * @return Status::ERR_FATAL todo. The sequence id is depletion. It must 
 * implement reuse system or extending id space.
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running 
 * transactions. Typical usage is in DDL to register sequence objects.
 */
extern Status create_sequence(SequenceId* id); // NOLINT

/**
 * @brief update sequence value and version
 * @details request shirakami to make the sequence value for the specified 
 * version durable together with the associated transaction.
 * @param[in] token the session token whose current transaction will be 
 * associated with the sequence value and version
 * @param[in] id the sequence id whose value/version will be updated
 * @param[in] version the version of the sequence value
 * @param[in] value the new sequence value
 * @return Status::OK if the update operation is successful
 * @return otherwise if any error occurs
 * @warning multiple update_sequence calls to a sequence with same version 
 * number cause undefined behavior.
 */
extern Status update_sequence(Token token, SequenceId id,
                              SequenceVersion version, SequenceValue value);

/**
 * @brief read sequence value
 * @details retrieve sequence value of the "latest" version from shirakami
 * Shirakami determines the latest version by finding maximum version number of
 * the sequence from the transactions that are durable at the time this 
 * function call is made. It's up to shirakami when to make transactions 
 * durable, so there can be delay of indeterminate length before update 
 * operations become visible to this function. As for concurrent update 
 * operations, it's only guaranteed that the version number retrieved by this 
 * function is equal or greater than the one that is previously retrieved.
 * @param[in] id the sequence id whose value/version are to be retrieved
 * @param [out] version the sequence's latest version number, that is valid 
 * only when this function is successful with Status::OK.
 * @param [out] value the sequence value, that is valid only when this function 
 * is successful with Status::OK.
 * @return Status::OK if the retrieval is successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running 
 * transactions. Typical usage is to retrieve sequence initial value at the 
 * time of database recovery.
 */
extern Status read_sequence(SequenceId id, SequenceVersion* version,
                            SequenceValue* value); // NOLINT

/**
 * @brief delete the sequence
 * @param[in] id the sequence id that will be deleted
 * @param[in] token
 * @return Status::OK if the deletion was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running 
 * transactions. Typical usage is in DDL to unregister sequence objects.
 */
extern Status delete_sequence(SequenceId id); // NOLINT

} // namespace shirakami