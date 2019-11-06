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

//extern char *gen_key_string(void);
//extern char *gen_value_string(void);

extern void kvs_upsert(Token token, char *key, uint len_key, char *val, uint len_val);
extern void kvs_delete(const Token token, char *key, uint len_key);

/**
 * @brief delete std::vector<Record*> DataBase at kvs_charkey/src/xact.cc
 * @return void
 */
extern void kvs_delete_database();
extern bool kvs_insert(const Token token, char *key, uint len_key, char *val, uint len_val);
extern bool kvs_update(Token token, char *key, uint len_key, char *val, uint len_val);
extern Tuple* kvs_search_key(Token token, char *key, uint len_key);
extern bool kvs_commit(const int token);
// inclusive/exclusive
// prefix scan
// suffix scan?->no
extern std::vector<Tuple*> kvs_scan_key(Token token, char *lkey, uint len_lkey, char *rkey, uint len_rkey);
//extern Tuple* make_tuple(char *key, uint len_key, char *val, uint len_val);
//extern Tuple* make_tuple(char *key, uint len_key);

extern void debug_print_key(void);

}  // namespace kvs
