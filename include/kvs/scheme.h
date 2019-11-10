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

#include "include/header.h"

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
  std::unique_ptr<char[]> key;
  std::unique_ptr<char[]> val;
  std::size_t len_key;
  std::size_t len_val;
  bool visible = true; // for delete, search

  Tuple() = default;
  ~Tuple() = default;

  Tuple(char const *key, std::size_t len_key, char const *val, std::size_t len_val) {
    this->len_key = len_key;
    this->len_val = len_val;
    this->visible = true;
    this->key = std::make_unique<char[]>(len_key);
    this->val = std::make_unique<char[]>(len_val);
    memcpy(this->key.get(), key, len_key);
    memcpy(this->val.get(), val, len_val);
  }

  Tuple(const Tuple& right) {
    this->len_key = right.len_key;
    this->len_val = right.len_val;
    this->visible = right.visible;
    this->key = std::make_unique<char[]>(right.len_key);
    this->val = std::make_unique<char[]>(right.len_val);
    memcpy(this->key.get(), right.key.get(), right.len_key);
    memcpy(this->val.get(), right.val.get(), right.len_val);
  }

  Tuple& operator=(const Tuple& right) {
    this->len_key = right.len_key;
    this->len_val = right.len_val;
    this->visible = right.visible;
    this->key = std::make_unique<char[]>(right.len_key);
    this->val = std::make_unique<char[]>(right.len_val);
    memcpy(this->key.get(), right.key.get(), right.len_key);
    memcpy(this->val.get(), right.val.get(), right.len_val);
    
    return *this;
  }
};
}  // namespace kvs

