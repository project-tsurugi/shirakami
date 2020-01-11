#pragma once
#include "scheme.h"

namespace kvs {
/**
 * @file
 * @brief transaction engine interface
 */

/**
 * @brief initialize kvs environment
 *
 * When it starts to use this system, in other words, it starts to build database, it must be executed first.
 */
extern void init();

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
 */
extern Status enter(Token& token);

/**
 * @brief leave session
 *
 * It return the objects which was got at enter function to kThreadTable.
 * @parm the token retrieved by enter()
 * @return Status::OK if successful
 * @return Status::WARN_NOT_IN_A_SESSION if the session is already ended.
 */
extern Status leave(Token token);

/**
 * @brief Processing of beginning transaction.
 *
 * It is need to decide the value of kReclamationEpoch.
 * If it isn't used correctly, garbage collection don't work correctly, then system will fail.
 *
 */
extern void tbegin(Token token);

/**
 * @brief silo's(SOSP2013) validation protocol.
 * @param the token retrieved by enter()
 * @pre executed enter -> tbegin -> transaction operation.
 * @post execute leave to leave the session or tbegin to start next transaction.
 * @return Status::ERR_VALIDATION This means read validation failure and it already executed abort(). After this, do tbegin to start next transaction or leave to leave the session.
 * @return Status::OK It commited correctly.
 */
extern Status commit(Token token);

/**
 * @brief abort and end the transaction.
 *
 * do local set clear, try gc.
 * @param token the token retrieved by enter()
 * @pre it did enter -> ... -> tbegin -> some access operation(update/insert/search/delete) or no operation
 * @post execute leave to leave the session or tbegin to start next transaction.
 * @return Status::OK It work correctly.
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
 * @return Status::ERR_NOT_FOUND if the storage is not registered with the given name
 */
extern Status get_storage(char const* name, std::size_t len_name, Storage& storage);

/**
 * @brief delete existing storage and records under the storage.
 * @param storage the storage handle retrieved with register_storage() or get_storage()
 * @return Status::OK if successful
 * @return Status::ERR_NOT_FOUND if the storage is not registered with the given name
 */
extern Status delete_storage(Storage storage);

/**
 * @brief update the record for the given key, or insert the key/value if the record does not exist
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the upserted record
 * @param len_key indicate the key length
 * @param val the value of the upserted record
 * @len_val indicate the value length
 * @return Status OK if successful
 * @return error otherwise
 */
extern Status upsert(Token token, Storage storage, const char* key, std::size_t len_key, const char* const val, std::size_t const len_val);

/**
 * @brief delete the record for the given key
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the record for deletion
 * @param len_key indicate the key length
 * @pre it already executed enter.
 * @post nothing. This function never do abort.
 * @return Status::ERR_NOT_FOUND no corresponding record in masstree. It executed abort, so retry the transaction please.
 * @return Status::OK if successful
 * @return Status::WARN_CANCEL_PREVIOUS_OPERATION it canceled an update/insert operation before this fucntion.
 */
extern Status delete_record(Token token, Storage storage, const char* key, std::size_t len_key);

/**
 * @brief Delete the all records.
 * @pre This function is called by a single thread and does't allow moving of other threads.
 * @return Status
 */
extern Status delete_all_records();

/**
 * @brief delete std::vector<Record*> kGarbageRecords at kvs_charkey/src/xact.cc
 * @return void
 */
extern void delete_all_garbage_records();

/**
 * @brief insert the record with given key/value
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the inserted record
 * @param len_key indicate the key length
 * @param val the value of the inserted record
 * @param len_val indicate the value length
 * @return Status::ERR_ALREADY_EXISTS if the record already exists for the given key
 * @return Status::OK success
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE it already executed update/insert, so it update the value which is going to be updated.
 * @return error otherwise
 */
extern Status insert(Token token, Storage storage, const char* key, std::size_t len_key, const char* const val, std::size_t const len_val);

/**
 * @brief update the record for the given key
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the updated record
 * @param len_key indicate the key length
 * @param val the value of the updated record
 * @param len_val indicate the value length
 * @return Status::ERR_NOT_FOUND no corresponding record in masstree. It executed abort, so retry the transaction please.
 * @return Status::OK if successful
 * @return Status::WARN_WRITE_TO_LOCAL_WRITE it already executed update/insert, so it update the value which is going to be updated.
 */
extern Status update(Token token, Storage storage, const char* key, std::size_t len_key, const char* const val, std::size_t const len_val);

/**
 * @brief search with the given key and return the found tuple
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the search key
 * @param len_key indicate the key length
 * @param tuple output parameter to pass the found Tuple pointer.
 * The ownership of the address which is pointed by the tuple is in kvs.
 * So upper layer from kvs don't have to be care.
 * nullptr when nothing is found for the given key.
 * @return Status::ERR_ILLEGAL_STATE it read the record which is inserted or deleted concurrently. it executed abort, so retry the transaction please.
 * @return Status::ERR_NOT_FOUND no corresponding record in masstree. It executed abort, so retry the transaction please.
 * @return Status::OK if successful
 * @return Status::WARN_ALREADY_DELETE it already executed delete operation.
 */
extern Status search_key(Token token, Storage storage, const char* key, std::size_t len_key, Tuple** const tuple);

/**
 * @brief search with the given key range and return the found tuples
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
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
 * @return Status::ERR_ILLEGAL_STATE it read the record which is inserted or deleted concurrently. it executed abort, so retry the transaction please.
 * @return Status::OK if successful
 */
extern Status scan_key(Token token, Storage storage,
    const char* lkey, std::size_t len_lkey, const bool l_exclusive,
    const char* rkey, std::size_t len_rkey, const bool r_exclusive,
    std::vector<Tuple*>& result);

/**
 * @brief This function do gc all records in all containers for gc.
 *
 * This function isn't thread safe.
 */
extern void forced_gc_all_records();

}  // namespace kvs
