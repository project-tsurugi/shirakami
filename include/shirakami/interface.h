/**
 * @file include/shirakami/interface.h
 * @brief transaction execution engine interface.
 */

#pragma once

#include <vector>

#include "api_storage.h"
#include "database_options.h"
#include "log_record.h"
#include "scheme.h"
#include "storage_options.h"
#include "transaction_options.h"
#include "transaction_state.h"
#include "tuple.h"

namespace shirakami {

/**
 * @brief transactional termination command about abort.
 * @details It is user abort, does cleaning for local set/cache, and try gc.
 * @param[in] token the token retrieved by enter()
 * @pre it did enter -> ... -> (tx_begin ->) some transactional operations 
 * (update / insert / upsert / search / delete) or no operation.
 * @return Status::OK success.
 */
extern Status abort(Token token); // NOLINT

/**
 * @brief close the scan which was opened at open_scan.
 * @param[in] token the token retrieved by enter().
 * @param[in] handle identify the specific scan which was opened at open_scan.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE The @b handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it 
 * can't execute it.
 */
extern Status close_scan(Token token, ScanHandle handle); // NOLINT

/**
 * @brief It checks this transaction can commit and executes commit.
 * @details If this function return ERR_... status, this called abort function 
 * implicitly. Otherwise, it commits.
 * @param[in] token retrieved by enter().
 * @param[in,out] cp commit parameter to notify commit timestamp and wait 
 * obeyed to commit_param.commit_property.
 * @pre you executed enter command, you didn't execute leave command.
 * @return Status::ERR_CONFLICT_ON_WRITE_PRESERVE This means validation failure
 * about write preserve by the transaction which is long tx mode.
 * @return Status::ERR_FAIL_INSERT It fails to commit due to failing insert 
 * operation of the transaction.
 * @return Status::ERR_FATAL Some programming error.
 * @return Status::ERR_PHANTOM This transaction can not commit due to phantom 
 * problem.
 * @return Status::ERR_WRITE_TO_DELETED_RECORD This transaction including update 
 * operations was interrupted by some delete transaction between read phase and 
 * validation phase.
 * @return Status::ERR_VALIDATION This means read validation failed.
 * @return Status::OK success.
 * @return Status::WARN_PREMATURE The long transaction must wait until the 
 * changing epoch to query some operation.
 * @return Status::WARN_WAITING_FOR_OTHER_TX The long transaction needs wait 
 * for finishing commit by other high priority tx.
 */
extern Status commit(Token token, commit_param* cp = nullptr); // NOLINT

/**
 * @brief NOT IMPLEMENTED NOW: It checks whether the transaction allocated 
 * commit_id at commit function was durable.
 * @param[in] token This should be the token which was used for commit function.
 * @param[in] commit_id This should be the commit_id which was received at 
 * commit function with @b token.
 * @return  true This transaction was committed from the point of view of 
 * recovery.
 * @return  false This transaction was not committed from the point of view of 
 * recovery.
 */
extern bool check_commit(Token token, std::uint64_t commit_id); // NOLINT

/**
 * @brief Delete the all records in all tables.
 * @pre This function is called by a single thread and doesn't allow concurrent 
 * processing by other threads. 
 * This is not DML operations but DDL operations.
 * @details  It must not call tx_begin(Token token) before this calling. And 
 * it doesn't need to call enter/leave around calling this function.
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
 * @return Status::WARN_CANCEL_PREVIOUS_INSERT This delete operation merely 
 * canceled an previous insert.
 * @return Status::WARN_CANCEL_PREVIOUS_UPDATE This delete operation merely 
 * canceled an previous update.
 * @return Status::WARN_CANCEL_PREVIOUS_UPSERT This delete operation merely 
 * canceled an previous upsert.
 * @return Status::WARN_CONFLICT_ON_WRITE_PRESERVE This function can't execute 
 * because this tx is short tx and found write preserve of long tx.
 * @return Status::WARN_ILLEGAL_OPERATION You execute delete_record on read only 
 * mode. So this operation was canceled.
 * @return Status::WARN_INVALID_HANDLE It is caused by executing this operation in 
 * read only mode.
 * @return Status::WARN_NOT_FOUND The target page is not found or deleted.
 * @return Status::WARN_PREMATURE The long transaction must wait until the 
 * changing epoch to query some operation.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::WARN_WRITE_WITHOUT_WP This function can't execute because 
 * this tx is long tx and didn't execute wp for @a storage.
 * @return Status::OK success.
 */
extern Status delete_record(Token token, Storage storage, // NOLINT
                            std::string_view key);

/**
 * @brief enter session
 * @param[out] token output parameter to return the token
 * @pre Maximum degree of parallelism of this function without leave is the size of 
 * session_table_, KVS_MAX_PARALLEL_THREADS.
 * @post When it ends this session, do leave(Token token).
 * @return Status::OK
 * @return Status::ERR_SESSION_LIMIT There are no capacity of session.
 */
extern Status enter(Token& token); // NOLINT

/**
 * @brief Confirm existence of the key in the @a storage.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage input parameter about the storage.
 * @param[in] key input parameter about the key.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete 
 * operation of this transaction.
 * @return Status::WARN_CONCURRENT_INSERT This search was interrupted by 
 * other's insert.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record 
 * by other updater, and it could not complete search.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you 
 * have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_PREMATURE In long tx mode, it have to wait for no 
 * transactions to be located in an order older than the order in which this 
 * transaction is located.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::ERR_CONFLICT_ON_WRITE_PRESERVE The short tx's read found long
 * tx's wp and executed abort command internally.
 * @return Status::ERR_FATAL programming error.
 */
extern Status exist_key(Token token, Storage storage, std::string_view key);

/**
 * @brief do delete operations for all records, join core threads, delete the
 * remaining garbage (heap) objects, and do remaining work.
 * @pre It already did init() and invoked core threads.
 * @param[in] force_shut_down_logging If true, interrupt logging and shut down. 
 * Otherwise wait for the end of logging.
 * @details It do delete operations for all records. init() did invoking core 
 * threads detached. 
 * So it should join those threads.
 * This function serves that joining after doing those delete operations.
 * Then, it delete the remaining garbage (heap) object by using private 
 * interface.
 * @return void
 */
extern void fin(bool force_shut_down_logging = true); // NOLINT

/**
 * @brief It initializes shirakami's environment.
 * @details When it starts or restarts this system, in other words, database, 
 * it must be executed first or after fin command.
 * If you don't be explicit log directory path by @a options, shirakami makes 
 * and uses temporally directory whose the directory name was named by using 
 * phrases: shirakami, process id, and value of timestamp counter. For example, 
 * shirakami-111-222.
 * @param[in] options Options about open mode and logging.
 * @return Status::OK
 * @return Status::WARN_ALREADY_INIT Since it have already called int, it have 
 * not done anything in this call.
 */
extern Status init(database_options options = {}); // NOLINT

/**
 * @brief insert the record with given key/value
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the inserted record
 * @param[in] val the value of the inserted record
 * @return Status::OK success. If this tx executed delete operation, this insert
 * change the operation into update operation which updates using @a val.
 * @return Status::WARN_ALREADY_EXISTS The records whose key is the same as @b key 
 * exists in db, so this function returned immediately. And it is treated that 
 * the read operation for the record was executed by this operation to depend on
 *  existing the record.
 * @return Status::WARN_CONCURRENT_INSERT This operation is canceled due to 
 * concurrent insert by other tx.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::ERR_PHANTOM The position (of node in in-memory tree indexing) 
 * which was inserted by this function was also read by previous scan 
 * operations, and it detects phantom problem by other transaction's write. 
 * It did abort().
 */
extern Status insert(Token token, Storage storage,
                     std::string_view key, // NOLINT
                     std::string_view val);

/**
 * @brief leave session
 * @details It return the objects which was got at enter function to
 * session_table_.
 * @param[in] token retrieved by enter()
 * @return Status::OK success.
 * @return Status::WARN_NOT_IN_A_SESSION The session may be already ended.
 * @return Status::ERR_INVALID_ARGS The @b token is invalid.
 */
extern Status leave(Token token); // NOLINT

/**
 * @brief This function preserve the specified range of masstree
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] l_key the left end key of range.
 * @param[in] l_end whether including the left end key for this range.
 * @param[in] r_key the right end key of range.
 * @param[in] r_end whether including the right end key for this range.
 * @param[out] handle the handle to identify scanned result. This handle will be
 * deleted at abort function or close_scan command.
 * @param[in] max_size Default is 0. If this argument is 0, it will not use 
 * this argument. This argument limits the number of results.
 * @attention This scan limits range which is specified by @b l_key, @b l_end, 
 * @b r_key, and @b r_end.
 * @return Status::ERR_FATAL programming error.
 * @return Status::OK success.
 * @return Status::WARN_SCAN_LIMIT The scan could find some records but could
 * not preserve result due to capacity limitation.
 * @return Status::WARN_NOT_FOUND The scan couldn't find any records.
 * @return Status::WARN_PREMATURE In long tx mode, it have to wait for some 
 * high priority transactions.
 */
extern Status open_scan(Token token, Storage storage, std::string_view l_key,
                        scan_endpoint l_end, std::string_view r_key,
                        scan_endpoint r_end, ScanHandle& handle,
                        std::size_t max_size = 0); // NOLINT

/**
 * @brief advance cursor
 * @details This function advances the cursor by one in the range opened by 
 * open_scan. It skips deleted record.
 * @param[in] token the token retrieved by enter()
 * @param[in] handle identify the specific open_scan.
 * @return Status::ERR_FATAL programming error.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE @a handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it 
 * can't execute it.
 * @return Status::WARN_SCAN_LIMIT The cursor already reached endpoint of scan.
 */
extern Status next(Token token, ScanHandle handle);

/**
 * @brief This reads the key of record pointed by the cursor.
 * 
 * @param[in] token the token retrieved by enter()
 * @param[in] handle identify the specific open_scan.
 * @param[out] key the result of this function.
 * @return Status::ERR_FATAL programming error.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE This transaction already executed 
 * delete_record for the same page.
 * @return Status::WARN_CONCURRENT_INSERT The target page is concurrently
 * inserted. Please wait to finish the concurrent transaction which is 
 * inserting the target page or call abort api call.
 * @return Status::WARN_CONCURRENT_UPDATE The target page is concurrently
 * updated. Please wait to finish the concurrent transaction which is updating
 * the target page or call abort api call.
 * @return Status::ERR_FAIL_WP Conflict on write preserve of high priority long
 * transaction. It executed abort command.
 * @return Status::WARN_INVALID_HANDLE @b handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it 
 * can't execute it.
 * @return Status::WARN_SCAN_LIMIT The cursor already reached endpoint of scan.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 */
extern Status read_key_from_scan(Token token, ScanHandle handle,
                                 std::string& key);

/**
 * @brief This reads the value of record pointed by the cursor.
 * 
 * @param[in] token the token retrieved by enter()
 * @param[in] handle identify the specific open_scan.
 * @param[out] value  the result of this function.
 * @return Status::ERR_FATAL programming error.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE This transaction already executed 
 * delete_record for the same page.
 * @return Status::WARN_CONCURRENT_INSERT The target page is concurrently
 * inserted. Please wait to finish the concurrent transaction which is 
 * inserting the target page or call abort api call.
 * @return Status::WARN_CONCURRENT_UPDATE The target page is concurrently
 * updated. Please wait to finish the concurrent transaction which is updating
 * the target page or call abort api call.
 * @return Status::ERR_FAIL_WP Conflict on write preserve of high priority long
 * transaction. It executed abort command.
 * @return Status::WARN_INVALID_HANDLE @b handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it 
 * can't execute it.
 * @return Status::WARN_SCAN_LIMIT The cursor already reached endpoint of scan.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 */
extern Status read_value_from_scan(Token token, ScanHandle handle,
                                   std::string& value);

/**
 * @brief This function checks the size resulted at open_scan with the @b handle.
 * @param[in] token the token retrieved by enter()
 * @param[in] handle the handle to identify scanned result. This handle will be deleted 
 * at abort function.
 * @param[out] size the size resulted at open_scan with the @a handle .
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE The @a handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it 
 * can't execute it.
 */
[[maybe_unused]] extern Status
scannable_total_index_size(Token token, ScanHandle handle,
                           std::size_t& size); // NOLINT

/**
 * @brief It searches with the given key and return the found tuple.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the search key
 * @param[out] value output parameter to pass the found Tuple pointer.
 * @return Status::ERR_FATAL programming error.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete 
 * operation of this transaction.
 * @return Status::WARN_CONCURRENT_INSERT This search was interrupted by 
 * other's insert.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record 
 * by other updater, and it could not complete search.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you 
 * have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_PREMATURE In long tx mode, it have to wait for no 
 * transactions to be located in an order older than the order in which this 
 * transaction is located.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::ERR_CONFLICT_ON_WRITE_PRESERVE The short tx's read found long
 * tx's wp and executed abort command internally.
 */
extern Status search_key(Token token, Storage storage, std::string_view key,
                         std::string& value); // NOLINT

/**
 * @brief Transaction begins.
 * @attention This function basically does not have to be called. 
 * Because it is called automatically internally using {token used for api, 
 * transaction_type::SHORT, {}}.
 * @details To determine the GC-capable epoch, determine the epoch at the start 
 * of the transaction. 
 * @param[in] options Transaction options. There are token got from enter 
 * command, transaction_type SHORT or LONG or READ_ONLY, and write_preserve for 
 * transaction_type LONG. Default is token:{}, transaction_type:{SHORT}, 
 * write_preserve:{}.
 * @attention If you specify read_only is true, you can not execute 
 * transactional write operation in this transaction.
 * @return Status::ERR_FAIL_WP Wp of this function failed. Retry from tx_begin.
 * @return Status::ERR_FATAL programming error.
 * @return Status::OK Success.
 * @return Status::WARN_ALREADY_BEGIN When it uses multiple tx_begin without 
 * termination command, this is returned.
 * @return Status::WARN_ILLEGAL_OPERATION You executed this command using @a 
 * write_preserve and not using long tx mode.
 */
extern Status tx_begin(transaction_options options = {}); // NOLINT

/**
 * @brief It updates the record for the given key.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the updated record
 * @param[in] val the value of the updated record
 * @return Status::OK Success.
 * @return Status::WARN_ALREADY_DELETE The target page was already deleted.
 * @return Status::WARN_ILLEGAL_OPERATION You execute delete_record on read only 
 * mode. So this operation was canceled.
 * @return Status::WARN_NOT_FOUND The record is not found.
 */
extern Status update(Token token, Storage storage, std::string_view key,
                     std::string_view val); // NOLINT

/**
 * @brief update the record for the given key, or insert the key/value if the
 * record does not exist
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the upserted record
 * @param[in] val the value of the upserted record
 * @return Status::ERR_PHANTOM The position (of node in in-memory tree indexing) 
 * which was inserted by this function was also read by previous scan operations, 
 * and it detects phantom problem by other transaction's write. It did abort().
 * @return Status::OK Success
 * @return Status::WARN_ILLEGAL_OPERATION You execute delete_record on read only 
 * mode. So this operation was canceled.
 * @return Status::WARN_INVALID_ARGS You tried to write to an area that was not 
 * wp in batch mode.
 * @return Status::WARN_STORAGE_NOT_FOUND The target storage of this operation 
 * is not found.
 */
extern Status upsert(Token token, Storage storage, std::string_view key,
                     std::string_view val); // NOLINT


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
 * @details the version number of the sequence that begins at 0 and increases monotonically.
 * For each version in the sequence, there is the associated value with it.
 */
using SequenceVersion = std::size_t;

/**
 * @brief create new sequence
 * @param [out] id the newly assigned sequence id, that is valid only 
 * when this function is successful with Status::OK.
 * @return Status::OK if the creation was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 */
extern Status create_sequence(SequenceId* id);

/**
 * @brief update sequence value and version
 * @details request shirakami to make the sequence value for the specified version 
 * durable together with the associated transaction.
 * @param token the session token whose current transaction will be associated 
 * with the sequence value and version
 * @param id the sequence id whose value/version will be updated
 * @param version the version of the sequence value
 * @param value the new sequence value
 * @return Status::OK if the update operation is successful
 * @return otherwise if any error occurs
 * @warning multiple update_sequence calls to a sequence with same version number 
 * cause undefined behavior.
 */
extern Status update_sequence(Token token, SequenceId id,
                              SequenceVersion version, SequenceValue value);

/**
 * @brief read sequence value
 * @details retrieve sequence value of the "latest" version from shirakami
 * Shirakami determines the latest version by finding maximum version number of
 * the sequence from the transactions that are durable at the time this function call is made.
 * It's up to shirakami when to make transactions durable, so there can be 
 * delay of indeterminate length before update operations become visible to this function. 
 * As for concurrent update operations, it's only guaranteed that the version number 
 * retrieved by this function is equal or greater than the one that is previously retrieved.
 * @param id the sequence id whose value/version are to be retrieved
 * @param [out] version the sequence's latest version number, that is valid only 
 * when this function is successful with Status::OK.
 * @param [out] value the sequence value, that is valid only when this function 
 * is successful with Status::OK.
 * @return Status::OK if the retrieval is successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is to retrieve sequence initial value at the time of database recovery.
 */
extern Status read_sequence(SequenceId id, SequenceVersion* version,
                            SequenceValue* value);

/**
 * @brief delete the sequence
 * @param[in] id the sequence id that will be deleted
 * @return Status::OK if the deletion was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is in DDL to unregister sequence objects.
 */
extern Status delete_sequence(SequenceId id);

//==========
/**
 * transaction state api.
 * These api is able at BUILD_WP=ON build mode.
 */

/**
 * @brief acquire transaction state handle.
 * @param[in] token The token of the transaction pointed to by the handle you 
 * want to acquire.
 * @param[out] handle The acquired handle.
 * @attention acquire_tx_state_handle and release_tx_state_handle should be 
 * called together. If you call one side more than other side, warning will 
 * be returned.
 * @pre The transaction linked @a token already executed @a tx_begin api.
 * @post Call release_tx_state_handle using @a handle.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_EXISTS This api was already called for this tx.
 * It updates @a handle by existing one.
 * @return Status::WARN_NOT_BEGIN The tx linked this session is not begun. So 
 * it can't acquire state handle.
 * @return Status::WARN_INVALID_ARGS If you call this api with using invalid 
 * @a token, this call returns this status.
 * @return Status::ERR_FATAL It occurs some programming error.
 */
Status acquire_tx_state_handle(Token token, TxStateHandle& handle);

/**
 * @brief release transaction state handle.
 * @param[in] handle The acquired handle by @a acquire_tx_state_handle.
 * @attention acquire_tx_state_handle and release_tx_state_handle should be 
 * called together. If you call one side more than other side, warning will 
 * be returned.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE If you call this api with using invalid 
 * handle @a handle, this call returns this status.
 */
Status release_tx_state_handle(TxStateHandle handle);

/**
 * @brief check the status of the transaction.
 * @param[in] handle The acquired handle by @a acquire_tx_state_handle.
 * @param[out] out The acquired status by this call.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE If you call this api with using invalid 
 * handle @a handle, this call returns this status.
 */
Status tx_check(TxStateHandle handle, TxState& out);

/**
 * @brief log event callback function type.
 * @details callback invoked on logging event on cc engine or datastore. The callback arguments are
 *   - the log worker number (0-origin index)
 *   - the log record begin pointer to iterate all the logged records
 *   - the log record end pointer to detect end position of the logged records
 */
using log_event_callback =
        std::function<void(std::size_t, log_record*, log_record*)>;

/**
 * @brief set logging event callback
 * @details register the callback invoked on the logging event (cc engine or 
 * datastore defines event timing)
 * @param handle the database handle
 * @param callback the callback to be invoked whose arguments are log worker 
 * number and iterator for log records
 * @return Status::OK if the call is successful
 * @return Status::WARN_INVALID_ARGS @a callback is not executable.
 */
Status database_set_logging_callback(log_event_callback const& callback);

//==========
/**
 * About datastore
 */

/**
 * @brief Get the datastore object which is used by shirakami engine.
 * @return void* 
 */
void* get_datastore();

//==========

} // namespace shirakami