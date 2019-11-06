#pragma once
#include "scheme.h"

namespace kvs {
/**
 * @file
 * @brief transaction engine interface
 */

/**
 * @brief initialize kvs environment
 */
extern void init();

/**
 * @brief enter session
 * @param token output parameter to return the token
 * @return Status::OK if successful
 * @return Status::WARN_ALREADY_IN_A_SESSION if the session is already started. 
 * Existing token is assigned to token parameter.
 */
extern Status enter(Token& token);

/**
 * @brief leave session
 * @parm the token retrieved by enter()
 * @return Status::OK if successful
 * @return Status::WARN_NOT_IN_A_SESSION if the session is already ended.
 */
extern Status leave(Token token);

/**
 * @brief silo's(SOSP2013) validation protocol.
 * @param the token retrieved by enter()
 * @return Status reporting success or fail
 */
extern Status commit(Token token);

/**
 * @brief abort and end the transaction
 * @param token the token retrieved by enter()
 * @return Status reporting success or fail
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
extern Status upsert(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val);

/**
 * @brief delete the record for the given key
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the record for deletion
 * @param len_key indicate the key length
 * @return Status ERR_NOT_FOUND if the record doesn't exist for the key
 * @return Status OK if successful
 * @return error otherwise
 */
extern Status delete_record(Token token, Storage storage, char const *key, std::size_t len_key);

/**
 * @brief delete std::vector<Record*> DataBase at kvs_charkey/src/xact.cc
 * @return void
 */
extern void delete_database();

/**
 * @brief insert the record with given key/value
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the inserted record
 * @param len_key indicate the key length
 * @param val the value of the inserted record
 * @param len_val indicate the value length
 * @return Status OK if successful
 * @return Status ERR_ALREADY_EXISTS if the record already exists for the given key
 * @return error otherwise
 */
extern Status insert(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val);

/**
 * @brief update the record for the given key
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the key of the updated record
 * @param len_key indicate the key length
 * @param val the value of the updated record
 * @param len_val indicate the value length
 * @return Status OK if successful
 * @return Status ERR_NOT_FOUND if the record does not exist for the given key
 * @return error otherwise
 */
extern Status update(Token token, Storage storage, char const *key, std::size_t len_key, char const *val, std::size_t len_val);

/**
 * @brief search with the given key and return the found tuple
 * @param token the token retrieved by enter()
 * @param storage the storage handle retrieved by register_storage() or get_storage()
 * @param key the search key
 * @param len_key indicate the key length
 * @param result output parameter to pass the found Tuple pointer.
 * nullptr when nothing is found for the given key.
 * TODO describe until when the returned tuple pointer is valid.
 * @return Status OK if successful
 * @return error otherwise
 */
extern Status search_key(Token token, Storage storage, char const *key, std::size_t len_key, Tuple** tuple);

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
 * TODO describe until when the returned tuple pointers are valid.
 * @return Status OK if successful
 */
extern Status scan_key(Token token, Storage storage,
    char const *lkey, std::size_t len_lkey, bool l_exclusive,
    char const *rkey, std::size_t len_rkey, bool r_exclusive,
    std::vector<Tuple*>& result);

extern void debug_print_key(void);

}  // namespace kvs
