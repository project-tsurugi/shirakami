#pragma once
#include "scheme.h"

namespace kvs {

//extern uint get_token(void);

/**
 * @file
 * @brief transaction engine interface
 */

/**
 * @brief initialize kvs environment
 */
extern void kvs_init(void);

/**
 * @brief enter session
 * @return token
 */

extern uint kvs_enter();

/**
 * @brief one thread starts handling kvs.
 * @return token number
 */
extern bool kvs_leave(uint token);

/**
 * @brief silo's(SOSP2013) validation protocol.
 * @return bool (success or fail)
 */
extern bool kvs_commit(const int token);

//extern char *gen_key_string(void);
//extern char *gen_value_string(void);

extern void kvs_upsert(uint token, char *key, uint len_key, char *val, uint len_val);
extern void kvs_delete(const uint token, char *key, uint len_key);

/**
 * @brief delete std::vector<Record*> DataBase at kvs_charkey/src/xact.cc
 * @return void
 */
extern void kvs_delete_database();
extern bool kvs_insert(const uint token, char *key, uint len_key, char *val, uint len_val);
extern bool kvs_update(uint token, char *key, uint len_key, char *val, uint len_val);
extern Tuple* kvs_search_key(uint token, char *key, uint len_key);
extern bool kvs_commit(const int token);
// inclusive/exclusive
// prefix scan
// suffix scan?->no
extern std::vector<Tuple*> kvs_scan_key(uint token, char *lkey, uint len_lkey, char *rkey, uint len_rkey);
//extern Tuple* make_tuple(char *key, uint len_key, char *val, uint len_val);
//extern Tuple* make_tuple(char *key, uint len_key);

extern void debug_print_key(void);

}  // namespace kvs
