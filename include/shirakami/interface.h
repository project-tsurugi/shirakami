/**
 * @file include/shirakami/interface.h
 * @brief transaction execution engine interface.
 */

#pragma once

#include <vector>

#include "scheme.h"
#include "tuple.h"

/**
 * @brief It is for logging to decide file name.
 */
#define STRING(macro) #macro // NOLINT
/**
 * @brief It is for logging to decide file name.
 */
#define MAC2STR(macro) STRING(macro) // NOLINT

namespace shirakami {

/**
 * @brief Create one table and return its handler.
 * @param[out] storage output parameter to pass the storage handle, that is used for the subsequent calls related with the storage.
 * Until delete_storage is called for the first time, multiple register_storage calls assign Storage value monotonically.
 * That is, Storage value assigned by register_storage is larger than the one assigned by previous call as long as no delete_storage is called.
 * Once delete_storage is called, Storage value can be recycled and there is no guarantee on the monotonicity.
 * @return Status::OK if successful.
 * @return Status::WARN_INVARIANT if the number of storages exceeds the maximum value of the handler.
 */
extern Status register_storage(Storage& storage);

/**
 * @brief Confirm existence of the storage.
 * @param[in] storage input parameter to confirm existence of the storage.
 * @return Status::OK if existence.
 * @return Status::WARN_NOT_FOUND if not existence.
 */
extern Status exist_storage(Storage storage);

/**
 * @brief delete existing storage and records under the storage.
 * @param[in] storage the storage handle retrieved with register_storage().
 * @return Status::OK if successful.
 * @return Status::WARN_INVALID_HANDLE if the storage is not registered with the given name.
 */
extern Status delete_storage(Storage storage);

/**
 * @brief Get a list of existing storage.
 * @param[out] out the list of existing storage.
 * @return Status::OK if successful.
 * @return Status::WARN_NOT_FOUND if no storage.
 */
extern Status list_storage(std::vector<Storage> &out);

/**
 * @brief transactional termination command about abort.
 * @details It aborts, does cleaning for local set/cache, and try gc.
 * @param[in] token the token retrieved by enter()
 * @pre it did enter -> ... -> (tx_begin ->) some transactional operations (update/insert/search/delete) or no
 * operation.
 * @post execute leave to leave the session or transactional operations (delete_record, insert, open_scan, ...etc) to
 * start next transaction.
 * @return Status::OK success.
 */
extern Status abort(Token token); // NOLINT

/**
 * @brief close the scan which was opened at open_scan.
 * @param[in] token the token retrieved by enter().
 * @param[in] handle identify the specific scan which was opened at open_scan.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE The @b handle is invalid.
 */
extern Status close_scan(Token token, ScanHandle handle); // NOLINT

/**
 * @brief It tries commit.
 * @details If this function return ERR_... status, this called abort function implicitly. Otherwise, it commits.
 * @param[in] token retrieved by enter().
 * @param[in,out] cp commit parameter to notify commit timestamp and wait obeyed to commit_param.commit_property.
 * @pre executed enter (-> tx_begin -> transaction operation).
 * @post execute leave to leave the session or transactional operations (ex. tx_begin, search, update, ...etc)  to start
 * next transaction.
 * @return Status::ERR_PHANTOM This transaction can not commit due to phantom problem, so it called abort().
 * @return Status::ERR_WRITE_TO_DELETED_RECORD This transaction including update operations was interrupted by some
 * delete transaction between read phase and validation phase. So it called abort.
 * @return Status::ERR_VALIDATION This means read validation failure and it already executed abort(). After this,
 * do tx_begin to start next transaction or leave to leave the session.
 * @return Status::OK success.
 */
extern Status commit(Token token, commit_param* cp = nullptr); // NOLINT

/**
 * @brief It checks whether the transaction allocated commit_id at commit function was committed.
 * @param[in] token This should be the token which was used for commit function.
 * @param[in] commit_id This should be the commit_id which was received at commit function with @b token.
 * @return  true This transaction was committed (durable).
 * @return  false This transaction was not committed (durable).
 */
extern bool check_commit(Token token, std::uint64_t commit_id); // NOLINT

/**
 * @brief Delete the all records in all tables.
 * @pre This function is called by a single thread and doesn't allow moving of other threads. This is not transactional
 * operation.
 * @details  It doesn't need to call tx_begin(Token token). Also it doesn't need to call enter/leave around calling this
 * function.
 * @return Status::OK success
 */
[[maybe_unused]] extern Status delete_all_records(); // NOLINT

/**
 * @brief delete the record for the given key
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the record for deletion
 * @pre it already executed enter.
 * @post nothing. This function never do abort.
 * @return Status::WARN_CANCEL_PREVIOUS_OPERATION it canceled an previous update / insert / upsert operation before
 * this function and did delete operation.
 * @return Status::WARN_INVALID_HANDLE It is caused by executing this operation in read only mode.
 * @return Status::WARN_NOT_FOUND No corresponding record in db. If you have problem by this, you should do abort.
 * @return Status::OK success.
 */
extern Status delete_record(Token token, Storage storage, std::string_view key); // NOLINT

/**
 * @brief enter session
 * @param[out] token output parameter to return the token
 * @pre Maximum degree of parallelism of this function without leave is the size of kThreadTable, KVS_MAX_PARALLEL_THREADS.
 * @post When it ends this session, do leave(Token token).
 * @return Status::OK
 * @return Status::ERR_SESSION_LIMIT There are no capacity of session.
 */
extern Status enter(Token& token); // NOLINT

/**
 * @brief do delete operations for all records, join core threads and delete the
 * remaining garbage (heap) objects.
 * @pre It already did init() and invoked core threads.
 * @details It do delete operations for all records. init() did invoking core threads detached. So it should join those
 * threads.
 * This function serves that joining after doing those delete operations.
 * Then, it delete the remaining garbage (heap) object by using private interface.
 * @return void
 */
extern void fin();

/**
 * @brief initialize shirakami environment
 * @details When it starts to use this system, in other words, it starts to
 * build database, it must be executed first.
 * @param[in] enable_recovery whether it is enable recovery from existing log.
 * @param[in] log_directory_path of WAL directory.
 * @return Status::ERR_INVALID_ARGS The args as a log directory path is invalid.
 * Some files which has the same path exist.
 * @return Status::OK
 */
extern Status init(bool enable_recovery = false, std::string_view log_directory_path = MAC2STR(PROJECT_ROOT)); // NOLINT

/**
 * @brief insert the record with given key/value
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the inserted record
 * @param[in] val the value of the inserted record
 * @return Status::ERR_PHANTOM The position (of node in in-memory tree indexing) which was inserted by this function was
 * also read by previous scan operations, and it detects phantom problem by other transaction's write. It did abort().
 * @return Status::OK success
 * @return Status::WARN_ALREADY_EXISTS The records whose key is the same as @b key exists in db, so this function
 * returned immediately.
 * @return Status::WARN_INVALID_HANDLE It is caused by executing this operation in read only mode.
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE it already executed update/insert/upsert, so it update the local write
 * set object.
 */
extern Status insert(Token token, Storage storage, std::string_view key, std::string_view val); // NOLINT

/**
 * @brief leave session
 * @details It return the objects which was got at enter function to
 * kThreadTable.
 * @param[in] token retrieved by enter()
 * @return Status::ERR_INVALID_ARGS The @b token is invalid.
 * @return Status::OK success.
 * @return Status::WARN_NOT_IN_A_SESSION The session may be already ended.
 */
extern Status leave(Token token); // NOLINT

/**
 * @brief This function preserve the specified range of masstree
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] l_key
 * @param[in] l_end
 * @param[in] r_key
 * @param[in] r_end
 * @param[out] handle the handle to identify scanned result. This handle will be
 * deleted at abort function.
 * @attention This scan limits range which is specified by @b l_key, @b l_end, @b r_key, and @b r_end.
 * @return Status::OK success.
 * @return Status::WARN_SCAN_LIMIT The scan could find some records but could
 * not preserve result due to capacity limitation.
 * @return Status::WARN_NOT_FOUND The scan couldn't find any records.
 */
extern Status open_scan(Token token, Storage storage, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, // NOLINT
                        scan_endpoint r_end, ScanHandle& handle);

/**
 * @brief This function reads the one records from the scan_cache which was created at open_scan function.
 * @details The read record is returned by @result.
 * @param[in] token the token retrieved by enter()
 * @param[in] handle input parameters to identify the specific scan_cache.
 * @param[out] result output parameter to pass the read record.
 * @return Status::ERR_PHANTOM This transaction can not commit due to phantom problem, so it called abort().
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by previous delete operation of this transaction.
 * @return Status::WARN_CONCURRENT_INSERT This scan was interrupted by other's insert.
 * @return Status::WARN_CONCURRENT_DELETE The read targets was deleted by delete operation of other transaction.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record by other updater, and it could not complete search.
 * @return Status::WARN_INVALID_HANDLE The @a handle is invalid.
 * @return Status::WARN_READ_FROM_OWN_OPERATION It read the records from it's preceding write (insert/update/upsert)
 * operation in the same tx.
 * @return Status::WARN_SCAN_LIMIT It have read all records in the scan_cache.
 */
extern Status read_from_scan(Token token, ScanHandle handle, Tuple** result); // NOLINT

/**
 * @brief search with the given key range and return the found tuples
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] l_key the key to indicate the beginning of the range, null if the beginning is open
 * @param[in] l_end indicate whether the @b l_key is exclusive (i.e. the record whose key equal to l_key is not included in the result).
 * @param[in] r_key the key to indicate the ending of the range, null if the end is open
 * @param[in] r_end indicate whether the @b r_key is exclusive
 * @param[out] result output parameter to pass the found Tuple pointers.
 * Empty when nothing is found for the given key range.
 * Returned tuple pointers are valid until commit/abort.
 * @return Status::ERR_PHANTOM This transaction can not commit due to phantom problem, so it called abort().
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete operation of this transaction.
 * @return Status::WARN_CONCURRENT_DELETE The read targets was deleted by delete operation.
 * @return Status::WARN_CONCURRENT_INSERT This scan was interrupted by other's insert.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record by other updater, and it could not complete search.
 */
extern Status scan_key(Token token, Storage storage, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, // NOLINT
                       scan_endpoint r_end, std::vector<const Tuple*>& result);

/**
 * @brief This function checks the size resulted at open_scan with the @b handle.
 * @param[in] token the token retrieved by enter()
 * @param[in] handle the handle to identify scanned result. This handle will be deleted at abort function.
 * @param[out] size the size resulted at open_scan with the @a handle .
 * @return Status::WARN_INVALID_HANDLE The @a handle is invalid.
 * @return Status::OK success.
 */
[[maybe_unused]] extern Status scannable_total_index_size(Token token, ScanHandle handle, std::size_t& size); // NOLINT

/**
 * @brief It searches with the given key and return the found tuple.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the search key
 * @param[out] tuple output parameter to pass the found Tuple pointer.
 * The ownership of the address which is pointed by the tuple is in shirakami.
 * So upper layer from shirakami don't have to be care.
 * nullptr when nothing is found for the given key.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete operation of this transaction.
 * @return Status::WARN_CONCURRENT_DELETE The read targets was deleted by delete operation of concurrent transaction.
 * @return Status::WARN_CONCURRENT_INSERT This search was interrupted by other's insert.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record by other updater, and it could not complete search.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_READ_FROM_OWN_OPERATION It read the records from it's preceding write (insert/update/upsert)
 * operation in the same tx.
 */
extern Status search_key(Token token, Storage storage, std::string_view key, Tuple** tuple); // NOLINT

/**
 * @brief Transaction begins.
 * @attention This function basically does not have to be called. Because it is called automatically internally using
 * the @b read_only (false) argument.
 * @details To determine the GC-capable epoch, determine the epoch at the start of the transaction. Specify true for
 * read_only to execute a fast read only transaction that just reads snapshots.
 * @param[in] token
 * @param[in] read_only If this is true, it uses read only mode which transactional reads read stale snapshot.
 * @attention If you specify read_only is true, you can not execute transactional write operation in this transaction.
 */
extern void tx_begin(Token token, bool read_only = false); // NOLINT

/**
 * @brief It updates the record for the given key.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the updated record
 * @param[in] val the value of the updated record
 * @return Status::OK if successful
 * @return Status::WARN_INVALID_HANDLE It is caused by executing this operation in read only mode.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should
 * do abort.
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE It already executed update/insert, so it update the value which is going
 * to be updated.
 */
extern Status update(Token token, Storage storage, std::string_view key, std::string_view val); // NOLINT

/**
 * @brief update the record for the given key, or insert the key/value if the
 * record does not exist
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the upserted record
 * @param[in] val the value of the upserted record
 * @return Status::ERR_PHANTOM The position (of node in in-memory tree indexing) which was inserted by this function was
 * also read by previous scan operations, and it detects phantom problem by other transaction's write. It did abort().
 * @return Status::OK success
 * @return Status::WARN_INVALID_HANDLE It is caused by executing this operation in read only mode.
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE It already did insert/update/upsert, so it overwrite its local write set.
 */
extern Status upsert(Token token, Storage storage, std::string_view key, std::string_view val); // NOLINT


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
 * @details the value of the sequence. Each value in the sequence is associated with some version number.
 */
using SequenceValue = std::int64_t;

/**
 * @brief sequence version
 * @details the version number of the sequence that begins at 0 and increases monotonically.
 * For each version in the sequence, there is the associated value with it.
 */
using SequenceVersion = std::size_t;

/**
 * @brief create new sequence
 * @param [out] id the newly assigned sequence id, that is valid only when this function is successful with Status::OK.
 * @return Status::OK if the creation was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 */
extern Status create_sequence(SequenceId* id);

/**
 * @brief update sequence value and version
 * @details request shirakami to make the sequence value for the specified version durable together
 * with the associated transaction.
 * @param token the session token whose current transaction will be associated with the sequence value and version
 * @param id the sequence id whose value/version will be updated
 * @param version the version of the sequence value
 * @param value the new sequence value
 * @return Status::OK if the update operation is successful
 * @return otherwise if any error occurs
 * @warning multiple update_sequence calls to a sequence with same version number cause undefined behavior.
 */
extern Status update_sequence(
        Token token,
        SequenceId id,
        SequenceVersion version,
        SequenceValue value);

/**
 * @brief read sequence value
 * @details retrieve sequence value of the "latest" version from shirakami
 * Shirakami determines the latest version by finding maximum version number of
 * the sequence from the transactions that are durable at the time this function call is made.
 * It's up to shirakami when to make transactions durable, so there can be delay of indeterminate length
 * before update operations become visible to this function. As for concurrent update operations, it's only guaranteed that
 * the version number retrieved by this function is equal or greater than the one that is previously retrieved.
 * @param id the sequence id whose value/version are to be retrieved
 * @param [out] version the sequence's latest version number, that is valid only when this function is
 * successful with Status::OK.
 * @param [out] value the sequence value, that is valid only when this function is successful with Status::OK.
 * @return Status::OK if the retrieval is successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is to retrieve sequence initial value at the time of database recovery.
 */
extern Status read_sequence(
        SequenceId id,
        SequenceVersion* version,
        SequenceValue* value);

/**
 * @brief delete the sequence
 * @param[in] token
 * @param id the sequence id that will be deleted
 * @return Status::OK if the deletion was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is in DDL to unregister sequence objects.
 */
extern Status delete_sequence(
        Token token,
        SequenceId id);

} // namespace shirakami