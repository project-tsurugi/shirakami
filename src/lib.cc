
#include "include/kernel.h"
#include "include/scheme.h"

#include <cstdint>

#include "kvs/debug.h"
#include "kvs/interface.h"

namespace kvs {

#define MAX_KEY_LEN (8)
#define MAX_VALUE_LEN (8)
extern std::vector<Record> DataBase;
extern char *gen_key_string(void);
extern char *gen_value_string(void);

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
}  // namespace kvs
