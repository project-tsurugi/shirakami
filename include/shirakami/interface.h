/**
 * @file include/shirakami/interface.h
 * @brief transaction execution engine interface.
 */

#pragma once

#include <vector>

#include "api_diagnostic.h"
#include "api_result.h"
#include "api_sequence.h"
#include "api_storage.h"
#include "api_tx_id.h"
#include "database_options.h"
#include "log_record.h"
#include "scheme.h"
#include "storage_options.h"
#include "transaction_options.h"
#include "transaction_state.h"
#include "tx_state_notification.h"

namespace shirakami {

/**
 * @brief transactional termination command about abort.
 * @details It is user abort, does cleaning for local set/cache, and try gc.
 * @param[in] token the token retrieved by enter()
 * @pre it did enter -> ... -> (tx_begin ->) some transactional operations
 * (update / insert / upsert / search / delete) or no operation.
 * @return Status::OK success.
 * @return Status::WARN_ILLEGAL_OPERATION After submitting commit, you must
 * wait the result.
 * @return Status::WARN_NOT_BEGIN This transaction was not begun.
 * @note Under normal circumstances, this function never returns errors other
 * than ones listed above.
 */
Status abort(Token token); // NOLINT

/**
 * @brief close the scan which was opened at open_scan.
 * @param[in] token the token retrieved by enter().
 * @param[in] handle identify the specific scan which was opened at open_scan.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE The @b handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it
 * can't execute it.
 */
Status close_scan(Token token, ScanHandle handle); // NOLINT

/**
 * @brief commit function with result notified by callback
 * @param token the target transaction control handle retrieved with enter().
 * @param callback the callback function invoked when (pre-)commit completes.
 * It's called exactly once. If this function returns false, caller must keep
 * the `callback` safely callable until its call, including not only the
 * successful commit but the case when transaction is aborted for some reason,
 * e.g. error with commit validation, or database is suddenly closed, etc.
 * After the callback invocation, the callback object passed as `callback`
 * parameter will be quickly destroyed.
 *
 * The callback receives following StatusCode:
 *   - Status::ERR_CC Error about concurrency control.
 *   - Status::ERR_KVS Error about key value store.
 *   - Status::OK success
 *   - Status::WARN_NOT_BEGIN This transaction was not begun.
 *   - Status::WARN_PREMATURE The long transaction must wait until the changing
 * epoch to query some operation.
 * On successful commit completion (i.e. Status::OK is passed)
 * durability_marker_type is available. Otherwise (and abort occurs on commit
 * try,) reason_code is available to indicate the abort reason.
 *
 * @return true if calling callback completed by the end of this function call
 * @return false otherwise (`callback` will be called asynchronously)
 */
bool commit(Token token, commit_callback_type callback);

/**
 * @brief old api. it is planed to remove from api.
 */
Status commit(Token token); // NOLINT

/**
 * @brief delete the record for the given key
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the record for deletion
 * @pre it already executed enter.
 * @post nothing. This function never do abort.
 * @return Status::WARN_CANCEL_PREVIOUS_INSERT This delete operation merely
 * canceled an previous insert.
 * @return Status::WARN_CANCEL_PREVIOUS_UPSERT This delete operation merely
 * canceled an previous upsert.
 * @return Status::WARN_CONFLICT_ON_WRITE_PRESERVE This function can't execute
 * because this tx is short tx and found write preserve of long tx.
 * @return Status::WARN_ILLEGAL_OPERATION You execute delete_record on read only
 * mode. So this operation was canceled.
 * @return Status::WARN_INVALID_HANDLE It is caused by executing this operation in
 * read only mode.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_BEGIN The transaction is not began.
 * @return Status::WARN_NOT_FOUND The target page is not found or deleted.
 * @return Status::WARN_PREMATURE The long transaction must wait until the
 * changing epoch to query some operation.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::WARN_WRITE_WITHOUT_WP This function can't execute because
 * this tx is long tx and didn't execute wp for @a storage.
 * @return Status::OK success.
 * @return Status::ERR_READ_AREA_VIOLATION error about read area.
 */
Status delete_record(Token token, Storage storage, // NOLINT
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
Status enter(Token& token); // NOLINT

/**
 * @brief Confirm existence of the key in the @a storage.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage input parameter about the storage.
 * @param[in] key input parameter about the key.
 * @return Status::OK success.
 * @return Status::WARN_CONCURRENT_INSERT The target page is being inserted.
 * The user can continue this transaction or end the transaction with
 * abort. If this page is unchanged at the time the transaction is requested to
 * commit, this operation will not cause a failure by the insert transaction.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record
 * by other updater, and it could not complete search.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you
 * have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_PREMATURE In long or read only tx mode, it have to wait
 * for no transactions to be located in an order older than the order in which
 * this transaction is located.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::ERR_CC Error about concurrency control.
 */
Status exist_key(Token token, Storage storage, std::string_view key);

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
void fin(bool force_shut_down_logging = true); // NOLINT

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
Status init(database_options options = {}); // NOLINT

/**
 * @brief insert the record with given key/value
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the inserted record
 * @param[in] val the value of the inserted record
 * @param[in] blobs_data blob references list used by the inserted record.
 * The blobs will be fully registered on datastore when transaction successfully commits.
 * Specify nullptr to pass empty list if the inserted record does not contain blob.
 * @param[in] blobs_size length of the blob references list
 * Specify 0 to pass empty list.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::OK success. If this tx executed delete operation, this insert
 * change the operation into update operation which updates using @a val.
 * @return Status::WARN_ALREADY_EXISTS The records whose key is the same as @b key
 * exists in db, so this function returned immediately. And it is treated that
 * the read operation for the record was executed by this operation to depend on
 *  existing the record.
 * @return Status::WARN_CONCURRENT_INSERT The target page is being inserted.
 * The user can continue this transaction or end the transaction with
 * abort. If this page is unchanged at the time the transaction is requested to
 * commit, this operation will not cause a failure by the insert transaction.
 * @return Status::WARN_ILLEGAL_OPERATION You execute insert on read only
 * mode. So this operation was canceled.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::WARN_WRITE_WITHOUT_WP This function can't execute because
 * this tx is long tx and didn't execute wp for @a storage.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::ERR_READ_AREA_VIOLATION error about read area.
 */
Status insert(Token token, Storage storage,
              std::string_view key,
              std::string_view val,
              blob_id_type const* blobs_data = nullptr,
              std::size_t blobs_size = 0
              );

/**
 * @brief leave session
 * @details It return the objects which was got at enter function to
 * session_table_.
 * @param[in] token retrieved by enter()
 * @return Status::OK success.
 * @return Status::WARN_NOT_IN_A_SESSION The session may be already ended.
 * @return Status::ERR_INVALID_ARGS The @b token is invalid.
 */
Status leave(Token token); // NOLINT

/**
 * @brief start scan and return the scan handle for the specified range.
 * @details This function preserve the specified range of masstree. If you use ltx
 * mode, it may log forwarding and read information.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] l_key the left end key of range. This argument is ignored if `l_end` is scan_endpoint::INF.
 * @param[in] l_end whether the scan range includes the left end key.
 * @param[in] r_key the right end key of range. This argument is ignored if `r_end` is scan_endpoint::INF.
 * @param[in] r_end whether the scan range includes the right end key.
 * @attention Contrary to yakushima where ranges are strictly validated, this function does not return an error for the
 * invalid range (condition that results in empty set, e.g. l_key > r_key.) and returns WARN_NOT_FOUND instead.
 * @param[out] handle the handle to identify scanned result. This handle will be deleted at abort function
 * or close_scan command.
 * @param[in] max_size limits the number of results. Default is 0. If this argument is 0, there is no limit.
 * @warning current implementation of `max_size` discards placeholder/tombstone records after fetching `max_size`
 * records from yakushima, so it's possible that the actual number of records fetched is less than `max_size` even
 * though there are plenty of records.
 * @param[in] right_to_left if true, the scan starts from right end to left. Otherwise, left end to right.
 * When this is set to true, current implementation has following limitation: 1. `max_size` must be set to 1
 * so that at most one entry is hit and returned as scan result 2. r_end must be scan_endpoint::INF so that the scan
 * is performed from unbounded right end. Status::ERR_FATAL is returned if these conditions are not met.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length should be equal or less than 30KB.
 * @return Status::WARN_MAX_OPEN_SCAN The fail due to the limits of number of concurrent open_scan without close_scan.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun.
 * @return Status::WARN_NOT_FOUND The scan couldn't find any records.
 * @return Status::WARN_PREMATURE In long or read only tx mode, it have to wait for some high priority transactions.
 * @return Status::WARN_STORAGE_NOT_FOUND The storage is not found.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::ERR_READ_AREA_VIOLATION error about read area.
 */
Status open_scan(Token token, Storage storage, std::string_view l_key,
                 scan_endpoint l_end, std::string_view r_key,
                 scan_endpoint r_end, ScanHandle& handle,
                 std::size_t max_size = 0,    // NOLINT
                 bool right_to_left = false); // NOLINT

/**
 * @brief advance cursor
 * @details This function advances the cursor by one in the range opened by
 * open_scan. It skips deleted record.
 * @param[in] token the token retrieved by enter()
 * @param[in] handle identify the specific open_scan.
 * @return Status::OK success.
 * @return Status::WARN_INVALID_HANDLE @a handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun.
 * So it can't execute it.
 * @return Status::WARN_SCAN_LIMIT The cursor already reached endpoint of scan.
 */
Status next(Token token, ScanHandle handle);

/**
 * @brief This reads the key of record pointed by the cursor.
 *
 * @param[in] token the token retrieved by enter()
 * @param[in] handle identify the specific open_scan.
 * @param[out] key the result of this function.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::OK success.
 * @return Status::WARN_CONCURRENT_INSERT The target page is being inserted.
 * The user can continue the scan with next api or end the transaction with
 * abort. If this page is unchanged at the time the transaction is requested to
 * commit, this read will not cause a failure.
 * @return Status::WARN_CONCURRENT_UPDATE The target page is concurrently
 * updated. Please wait to finish the concurrent transaction which is updating
 * the target page or call abort api call.
 * @return Status::WARN_INVALID_HANDLE @b handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it
 * can't execute it.
 * @return Status::WARN_SCAN_LIMIT The cursor already reached endpoint of scan.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 */
Status read_key_from_scan(Token token, ScanHandle handle, std::string& key);

/**
 * @brief This reads the value of record pointed by the cursor.
 *
 * @param[in] token the token retrieved by enter()
 * @param[in] handle identify the specific open_scan.
 * @param[out] value  the result of this function.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::OK success.
 * @return Status::WARN_CONCURRENT_INSERT The target page is being inserted.
 * The user can continue the scan with next api or end the transaction with
 * abort. If this page is unchanged at the time the transaction is requested to
 * commit, this read will not cause a failure.
 * @return Status::WARN_CONCURRENT_UPDATE The target page is concurrently
 * updated. Please wait to finish the concurrent transaction which is updating
 * the target page or call abort api call.
 * @return Status::WARN_INVALID_HANDLE @b handle is invalid.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun. So it
 * can't execute it.
 * @return Status::WARN_SCAN_LIMIT The cursor already reached endpoint of scan.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 */
Status read_value_from_scan(Token token, ScanHandle handle, std::string& value);

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
[[maybe_unused]] Status scannable_total_index_size(Token token,
                                                   ScanHandle handle,
                                                   std::size_t& size); // NOLINT

/**
 * @brief It searches with the given key and return the found tuple.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the search key
 * @param[out] value output parameter to pass the found Tuple pointer.
 * @return Status::OK success.
 * @return Status::WARN_CONCURRENT_INSERT The target page is being inserted.
 * The user can continue this transaction or end the transaction with
 * abort. If this page is unchanged at the time the transaction is requested to
 * commit, this operation will not cause a failure by the insert transaction.
 * @return Status::WARN_CONCURRENT_UPDATE This search found the locked record
 * by other updater, and it could not complete search.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you
 * have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_PREMATURE In long or read only tx mode, it have to wait
 * for no transactions to be located in an order older than the order in which
 * this transaction is located.
 * @return Status::WARN_STORAGE_NOT_FOUND @a storage is not found.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::ERR_READ_AREA_VIOLATION error about read area.
 */
Status search_key(Token token, Storage storage, std::string_view key,
                  std::string& value); // NOLINT

/**
 * @brief Transaction begins.
 * @attention This function must be called before requesting any other operation
 * for the new transaction. Otherwise, Status::WARN_NOT_BEGIN will be returned
 * for those requests.
 * @details To determine the GC-capable epoch, determine the epoch at the start
 * of the transaction.
 * @param[in] options Transaction options. There are token got from enter
 * command, options.transaction_type_ SHORT or LONG or READ_ONLY,
 * options.write_preserve_ for ltx, and options.read_area_ for improving ltx
 * performance. Default{} is token_:{}, transaction_type_:{SHORT},
 * write_preserve_:{}, read_area_:{}.
 * @attention If you specify read_only is true, you can not execute
 * transactional write operation in this transaction.
 * @return Status::OK Success.
 * @return Status::WARN_ALREADY_BEGIN When it uses multiple tx_begin without
 * termination command, this is returned.
 * @return Status::WARN_ILLEGAL_OPERATION You executed this command using @a
 * write_preserve and not using long tx mode.
 * @return Status::WARN_INVALID_ARGS User used storages not existed.
 */
Status tx_begin(transaction_options options = {}); // NOLINT

Status tx_clone(Token new_tx, Token from_tx);

/**
 * @brief It updates the record for the given key.
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the updated record
 * @param[in] val the value of the updated record
 * @param[in] blobs_data blob references list used by the updated record.
 * The blobs will be fully registered on datastore when transaction successfully commits.
 * Specify nullptr to pass empty list if the updated record does not contain blob.
 * @param[in] blobs_size length of the blob references list
 * Specify 0 to pass empty list.
 * @return Status::OK Success.
 * @return Status::WARN_ILLEGAL_OPERATION You execute update on read only
 * mode. So this operation was canceled.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun.
 * @return Status::WARN_NOT_FOUND The record is not found.
 * @return Status::WARN_WRITE_WITHOUT_WP This function can't execute because
 * this tx is long tx and didn't execute wp for @a storage.
 * @return Status::ERR_READ_AREA_VIOLATION error about read area.
 */
Status update(Token token, Storage storage, std::string_view key, std::string_view val,
              blob_id_type const* blobs_data = nullptr,
              std::size_t blobs_size = 0);

/**
 * @brief update the record for the given key, or insert the key/value if the
 * record does not exist
 * @param[in] token the token retrieved by enter()
 * @param[in] storage the handle of storage.
 * @param[in] key the key of the upserted record
 * @param[in] val the value of the upserted record
 * @param[in] blobs_data blob references list used by the upserted record.
 * The blobs will be fully registered on datastore when transaction successfully commits.
 * Specify nullptr to pass empty list if the upserted record does not contain blob.
 * @param[in] blobs_size length of the blob references list
 * Specify 0 to pass empty list.
 * @return Status::ERR_CC Error about concurrency control.
 * @return Status::OK Success
 * @return Status::WARN_ILLEGAL_OPERATION You execute upsert on read only
 * mode. So this operation was canceled.
 * @return Status::WARN_INVALID_ARGS You tried to write to an area that was not
 * wp in batch mode.
 * @return Status::WARN_INVALID_KEY_LENGTH The @a key is invalid. Key length
 * should be equal or less than 30KB.
 * @return Status::WARN_NOT_BEGIN The transaction was not begun.
 * @return Status::WARN_STORAGE_NOT_FOUND The target storage of this operation
 * is not found.
 * @return Status::WARN_WRITE_WITHOUT_WP This function can't execute because
 * this tx is long tx and didn't execute wp for @a storage.
 */
Status upsert(Token token, Storage storage, std::string_view key, std::string_view val,
              blob_id_type const* blobs_data = nullptr,
              std::size_t blobs_size = 0);


//==========
/**
 * transaction state api.
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
Status check_tx_state(TxStateHandle handle, TxState& out);

/**
 * @brief check whether the ltx has highest priority.
 * @pre This must be called between tx begin and termination (commit/abort).
 * If you don't save this rule, it is undefined behavior.
 * @param[in] token The token of the transaction.
 * @param[out] out whether the ltx has highest priority. If this is true, the
 * transaction has highest priority.
 * @return Status::OK success.
 * @return Status::WARN_NOT_BEGIN The transaction is not began.
 * @return Status::WARN_INVALID_ARGS The transaction is not long transaction mode.
*/
Status check_ltx_is_highest_priority(Token token, bool& out);

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
