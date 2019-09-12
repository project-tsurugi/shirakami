#include "kernel.h"
#include "debug.h"
//#include "port.h"
#include <cstdint>
//#include "kvs.h"
#include "interface.h"

#define MAX_KEY_LEN (8)
#define MAX_VALUE_LEN (8)
extern std::vector<Record> DataBase;

extern Record
get_new_row(void)
{
  Record rec;
  rec.tuple.key = gen_key_string();
  rec.tuple.val = gen_value_string();

  return rec;
}

void
lock_mutex(pthread_mutex_t *mutex)
{
	int ret;
	ret = pthread_mutex_lock(mutex);
	if (ret != 0) ERR;
}

void
unlock_mutex(pthread_mutex_t *mutex)
{
	int ret;
	ret = pthread_mutex_unlock(mutex);
	if (ret != 0) ERR;
}

extern char *
gen_string(const uint max_len)
{
  int len = rand() % max_len + 1;
  char *str = (char *)calloc(1, (len + 1));
  for (int i = 0; i < len; i++) {
    str[i] = 'a' + rand() % 24;
  }

  return str;
}

extern char *
gen_key_string(void)
{
  int len = rand() % MAX_KEY_LEN + 1;
  char *str = (char *)calloc(1, (len + 1));
  for (int i = 0; i < len; i++) {
    str[i] = 'a' + rand() % 24;
  }

  return str;
}

extern char *
gen_value_string(void)
{
  int len = rand() % MAX_VALUE_LEN;
  char *str = (char *)calloc(1, (len + 1));
  for (int i = 0; i < len; i++) {
    str[i] = 'a' + (rand() % 24);
  }

  return str;
}

/*
extern Tuple*
make_tuple(char *key, uint len_key, char *val, uint len_val)
{
  Tuple* tuple;

  tuple.len_key = len_key;
  if ((tuple.key = (char *)calloc(tuple.len_key, sizeof(char))) == NULL) ERR;
  memcpy(tuple.key, key, tuple.len_key);

  tuple.len_val = len_val;
  if ((tuple.val = (char *)calloc(tuple.len_val, sizeof(char))) == NULL) ERR;
  memcpy(tuple.val, val, tuple.len_val);

  return tuple;
}

extern Tuple*
make_tuple(char *key, uint len_key)
{
  Tuple tuple;

  tuple.len_key = len_key;
  if ((tuple.key = (char *)calloc(tuple.len_key, sizeof(char))) == NULL) ERR;
  memcpy(tuple.key, key, tuple.len_key);

  tuple.len_val = 0;
  tuple.val = nullptr;

  return tuple;
}
*/
