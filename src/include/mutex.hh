#pragma once

#include "debug.hh"

#include "header.hh"

namespace kvs {

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
