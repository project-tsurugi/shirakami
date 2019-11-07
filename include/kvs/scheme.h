#pragma once

#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <cstring>
#include <sys/time.h>
#include <pthread.h>
#include <iostream>
#include <unistd.h>
#include <time.h>
#include <iostream>
#include <algorithm>
#include <assert.h>
#include <vector>

#include "debug.h"

namespace kvs {

/**
 * @brief Session token
 */
using Token = std::uint64_t;

/**
 * @brief Storage Handle
 */
using Storage = std::uint64_t;

enum class Status : std::int32_t {
  // example of status code - remove/add more
  WARN_ALREADY_IN_A_SESSION = 2,
  WARN_NOT_IN_A_SESSION = 1,
  OK = 0,
  ERR_UNKNOWN = -1,
  ERR_NOT_FOUND = -2,
  ERR_ALREADY_EXISTS = -3,
  ERR_INVALID_ARGS = -4,
  ERR_ILLEGAL_STATE = -5,
  ERR_VALIDATION = -6,
};

typedef enum {
  SEARCH, UPDATE, INSERT, DELETE, UPSERT,
  BEGIN, COMMIT, ABORT} OP_TYPE;

class Tuple {
public:
  char *key = nullptr;
  char *val = nullptr;
  std::size_t len_key;
  std::size_t len_val;
  bool visible; // for delete, search

  Tuple() {
    this->visible = true;
  }

  ~Tuple() {
    free(this->key);
    free(this->val);
  }

  Tuple(char const *key, std::size_t len_key, char const *val, std::size_t len_val) {
    this->len_key = len_key;
    this->len_val = len_val;
    this->visible = true;
    if (!(this->key = (char *)malloc(len_key))) ERR;
    if (!(this->val = (char *)malloc(len_val))) ERR;
    memcpy(this->key, key, len_key);
    memcpy(this->val, val, len_val);
  }

  Tuple& operator=(const Tuple& rhs) {
    this->len_key = rhs.len_key;
    this->len_val = rhs.len_val;
    this->visible = rhs.visible;
    if (!(this->key = (char *)malloc(this->len_key))) ERR;
    if (!(this->val = (char *)malloc(this->len_val))) ERR;
    memcpy(this->key, rhs.key, this->len_key);
    memcpy(this->val, rhs.val, this->len_val);
    
    return *this;
  }
};
}  // namespace kvs

