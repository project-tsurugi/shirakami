#pragma once
#include "scheme.h"
#include "tuple.h"

#define STRING(macro) #macro
#define MAC2STR(macro) STRING(macro)

namespace kvs {
/**
 * @file
 * @brief transaction engine interface
 */

/**
 * @brief initialize kvs environment
 * @detail When it starts to use this system, in other words, it starts to build database, it must be executed first.
 * @param [in] log_directory path of WAL directory.
 * @return Status::ERR_INVALID_ARGS The args as a log directory path is invalid. 
 * Some files which has the same path exist.
 * @return Status::OK
 */
extern Status init(std::string log_directory_path = MAC2STR(PROJECT_ROOT));

/**
 * @brief join core threads.
 * @pre It already did init() and invoked core threads.
 * @details init() did invoking core threads detached. So it is good to join those threads. This function surves that joining.
 */
extern void fin();

/**
 * @brief enter session
 * @param [out] token output parameter to return the token
 * @pre Maximum degree of parallelism of this function without leave is the size of kThreadTable, KVS_MAX_PARALLEL_THREADS.
 * @post When it ends this session, do leave(Token token).
 * @return Status::OK
 * @return Status::ERR_SESSION_LIMIT There are no capacity of session.
 */
extern Status enter(Token& token);

/**
 * @brief leave session
 *
 * It return the objects which was got at enter function to kThreadTable.
 * @parm the token retrieved by enter()
 * @return Status::OK if successful
 * @return Status::WARN_NOT_IN_A_SESSION If the session is already ended.
 */
extern Status leave(Token token);

/**
 * @brief silo's(SOSP2013) validation protocol. If this function return ERR_ status, this called abort function.
 * @param the token retrieved by enter()
 * @pre executed enter -> tbegin -> transaction operation.
 * @post execute leave to leave the session or tbegin to start next transaction.
 * @return Status::ERR_VALIDATION This means read validation failure and it already executed abort(). After this, do tbegin to start next transaction or leave to leave the session.
 * @return Status::ERR_WRITE_TO_DELETED_RECORD This transaction was interrupted by some delete transaction between read phase and validation phase. So it called abort.
 * @return Status::OK success.
 */
extern Status commit(Token token);

/**
 * @brief abort and end the transaction.
 *
 * do local set/cache clear, try gc.
 * @param token [in] the token retrieved by enter()
 * @pre it did enter -> ... -> tbegin -> some access operation(update/insert/search/delete) or no operation
 * @post execute leave to leave the session or tbegin to start next transaction.
 * @return Status::OK success.
 */
extern Status abort(Token token);

/**
 * @brief register new storage, which is used to separate the KVS's key space,
 * any records in the KVS belong to only one storage
 * @param the name of the storage
 * @param len_name the length of the name
 * @param storage output parameter to pass the storage handle, 
 * that is used for the subsequent calls related with the storage.
 * @return Status::OK if successful
 */
extern Status register_storage(char const* name, std::size_t len_name, Storage& storage);

/**
 * @brief get existing storage handle
 * @param name the name of the storage
 * @param len_name the length of the name
 * @param storage output parameter to pass the storage handle,
 * that is used for the subsequent calls related with the storage.
 * @return Status::OK if successful
 * @return Status::ERR_NOT_FOUND If the storage is not registered with the given name
 */
extern Status get_storage(char const* name, std::size_t len_name, Storage& storage);

/**
 * @brief delete existing storage and records under the storage.
 * @param storage [in] the storage handle retrieved with register_storage() or get_storage()
 * @return Status::OK if successful
 * @return Status::ERR_NOT_FOUND If the storage is not registered with the given name
 */
extern Status delete_storage(Storage storage);

/**
 * @brief update the record for the given key, or insert the key/value if the record does not exist
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the upserted record
 * @param len_key indicate the key length
 * @param val the value of the upserted record
 * @len_val indicate the value length
 * @return Status::OK success
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE It already did insert/update/upsert, so it overwrite its local write set.
 */
extern Status upsert(Token token, Storage storage, const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val);

/**
 * @brief delete the record for the given key
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the record for deletion
 * @param len_key indicate the key length
 * @pre it already executed enter.
 * @post nothing. This function never do abort.
 * @return Status::WARN_NOT_FOUND No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::OK success.
 * @return Status::WARN_CANCEL_PREVIOUS_OPERATION it canceled an update/insert operation before this fucntion and did delete operation.
 */
extern Status delete_record(Token token, Storage storage, const char* const key, const std::size_t len_key);

/**
 * @brief Delete the all records.
 * @pre This function is called by a single thread and does't allow moving of other threads.
 * @detail This function executes tbegin(Token token) internaly, so it doesn't need to call tbegin(Token token).
 * Also it doesn't need to call enter/leave around calling this function.
 * Because this function calls enter/leave appropriately.
 * @return Status::WARN_ALREADY_DELETE There are no records.
 * @return Status::OK success
 * @return Return value of commit function. If it return this, you can retry delete_all_records meaning to resume this function.
 */
extern Status delete_all_records();

/**
 * @brief delete std::vector<Record*> kGarbageRecords at kvs_charkey/src/xact.cc
 * @return void
 */
extern void delete_all_garbage_records();

extern void delete_all_garbage_values();

/**
 * @brief insert the record with given key/value
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the inserted record
 * @param len_key indicate the key length
 * @param val the value of the inserted record
 * @param len_val indicate the value length
 * @return Status::WARN_ALREADY_EXISTS The records whose key is the same as @key exists in MTDB, so this function returned immediately.
 * @return Status::OK success
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE it already executed update/insert/upsert, so it update the local write set object.
 */
extern Status insert(Token token, Storage storage, const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val);

/**
 * @brief update the record for the given key
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the updated record
 * @param len_key indicate the key length
 * @param val the value of the updated record
 * @param len_val indicate the value length
 * @return Status::OK if successful
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE It already executed update/insert, so it update the value which is going to be updated.
 */
extern Status update(Token token, Storage storage, const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val);

/**
 * @brief search with the given key and return the found tuple
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param key the search key
 * @param len_key indicate the key length
 * @param tuple output parameter to pass the found Tuple pointer.
 * The ownership of the address which is pointed by the tuple is in kvs.
 * So upper layer from kvs don't have to be care.
 * nullptr when nothing is found for the given key.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete operation of this transaction.
 * @return Status::WARN_NOT_FOUND no corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
 * @return Status::WARN_CONCURRENT_DELETE The read targets was deleted by delete operation of concurrent transaction.
 */
extern Status search_key(Token token, Storage storage, const char* const key, const std::size_t len_key, Tuple** const tuple);

/**
 * @brief search with the given key range and return the found tuples
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param lkey the key to indicate the beginning of the range, null if the beginning is open
 * @param lkey_len indicate the lkey length
 * @param l_exclusive indicate whether the lkey is exclusive 
 * (i.e. the record whose key equal to lkey is not included in the result)
 * @param rkey the key to indicate the ending of the range, null if the end is open
 * @param rkey_len indicate the rkey length
 * @param r_exclusive indicate whether the rkey is exclusive
 * @param result output parameter to pass the found Tuple pointers.
 * Empty when nothing is found for the given key range.
 * Returned tuple pointers are valid untill commit/abort.
 * @return Status::OK success.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete operation of this transaction.
 * @return Status::WARN_CONCURRENT_DELETE The read targets was deleted by delete operation.
 */
extern Status scan_key(Token token, Storage storage,
    const char* const lkey, const std::size_t len_lkey, const bool l_exclusive,
    const char* const rkey, const std::size_t len_rkey, const bool r_exclusive,
    std::vector<const Tuple*>& result);

/**
 * @brief This function preserve the specified range of masstree
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param handle [out] the handle to identify scanned result. This handle will be deleted at abort function.
 * @return Status:::WARN_SCAN_LIMIT The scan could find some records but could not preserve result due to capacity limitation.
 * @return Status::WARN_NOT_FOUND The scan couldn't find any records.
 * @return Status::OK the some records was scanned.
 */
extern Status open_scan(Token token, Storage storage,
    const char* const lkey, const std::size_t len_lkey, const bool l_exclusive,
    const char* const rkey, const std::size_t len_rkey, const bool r_exclusive,
    ScanHandle& handle);

/**
 * @brief This function checks the size resulted at open_scan with the @handle .
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param handle [in] the handle to identify scanned result. This handle will be deleted at abort function.
 * @param size [out] the size resulted at open_scan with the @handle .
 * @return Status::WARN_INVALID_HANDLE The @handle is invalid.
 * @return Status::OK success.
 */
extern Status scannable_total_index_size(Token token, Storage storage, ScanHandle& handle, std::size_t& size);

/**
 * @brief This function reads the one records from the scan_cache 
 * which was created at open_scan function.
 * @details The read record is returned by @result.
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param handle [in] input parameters to identify the specific scan_cache.
 * @pram result [out] output parmeter to pass the read record.
 * @return Status::WARN_ALREADY_DELETE The read targets was deleted by delete operation of this transaction.
 * @return Status::WARN_CONCURRENT_DELETE The read targets was deleted by delete operation.
 * @return Status::WARN_INVALID_HANDLE The @handle is invalid.
 * @return Status::WARN_READ_FROM_OWN_OPERATION It read the records from it's preceding write (insert/update/upsert) operation in the same tx.
 * @return Status::WARN_SCAN_LIMIT It have read all records in the scan_cache.
 * @return Status::OK It succeeded.
 */
extern Status read_from_scan(Token token, Storage storage, const ScanHandle handle, Tuple** const result);

/**
 * @brief close the specified scan_cache
 * @param token [in] the token retrieved by enter()
 * @param storage [in] the storage handle retrieved by register_storage() or get_storage()
 * @param handle [in] identify the specific scan_cache.
 * @return Status::OK It succeeded. 
 * @return Status::WARN_INVALID_HANDLE The @handle is invalid.
 */
extern Status close_scan(Token token, Storage storage, const ScanHandle handle);

/**
 * @brief Recovery by single thread.
 * @detail This function isn't thread safe.
 * @pre It must decide correct wal directory name decided by change_wal_directory function before it executes recovery.
 */
extern void single_recovery_from_log();

/**
 * @brief This function do gc all records in all containers for gc.
 *
 * This function isn't thread safe.
 */
extern void forced_gc_all_records();

}  // namespace kvs
