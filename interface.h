//extern uint get_token(void);
extern void kvs_init(void);
extern uint kvs_enter();
extern bool kvs_leave(uint token);
extern bool kvs_commit(const int token);
//extern char *gen_key_string(void);
//extern char *gen_value_string(void);
extern void kvs_upsert(uint token, char *key, uint len_key, char *val, uint len_val);
extern void kvs_delete(const uint token, char *key, uint len_key);
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

