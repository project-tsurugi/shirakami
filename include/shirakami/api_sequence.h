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
 * @return Status::ERR_FATAL Some case. 1: The sequence id is depletion. It 
 * must implement reuse system or extending id space. 2: Programming error.
 * 
 * @note This function is not intended to be called concurrently with running 
 * transactions. Typical usage is in DDL to register sequence objects.
 */
extern Status create_sequence(SequenceId* id); // NOLINT

/**
 * @brief update sequence value and version.
 * @details request shirakami to make the sequence value for the specified 
 * version durable together with the associated transaction. So If you want to 
 * reflect this function effect, you must execute commit command and success
 * the commit because any transaction can't be decided serialize information 
 * without success termination. If you didn't begin transaction using @a token,
 * shirakami begin transaction using @a token with occ transaction state.
 * @param[in] token the session token whose current transaction will be 
 * associated with the sequence value and version.
 * @param[in] id the sequence id whose value / version will be updated.
 * @param[in] version the version of the sequence value.
 * @param[in] value the new sequence value.
 * @return Status::OK if the update sequence is cached for the transaction of 
 * @a token.
 * @return Status::WARN_ALREADY_EXIST The @a id is less than or equal to latest 
 * id of update_sequence operations of this transaction.
 * @return Status::WARN_NOT_BEGIN The transaction is not began. 
 * Start with short
 * mode and try it.
 * @return Status::ERR_FATAL Programming error.
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
 * @return Status::WARN_NOT_FOUND There is a 2 cases. 1: There isn't a sequence
 * object whose id is the same to @a id. 2: There is a target sequence object, 
 * but there isn't durable data.
 * @note This function is not intended to be called concurrently with running 
 * transactions. Typical usage is to retrieve sequence initial value at the 
 * time of database recovery.
 */
extern Status read_sequence(SequenceId id, SequenceVersion* version,
                            SequenceValue* value); // NOLINT

/**
 * @brief delete the sequence
 * @param[in] id the sequence id that will be deleted
 * @return Status::OK if the deletion was successful
 * @return Status::WARN_NOT_FOUND There isn't a sequence object whose id is 
 * the same to @a id.
 * @return Status::ERR_FATAL Programming error.
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running 
 * transactions. Typical usage is in DDL to unregister sequence objects.
 */
extern Status delete_sequence(SequenceId id); // NOLINT

} // namespace shirakami