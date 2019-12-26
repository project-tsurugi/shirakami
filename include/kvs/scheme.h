#pragma once

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>

namespace kvs {

/**
 * @brief Session token
 */
using Token = void*;

/**
 * @brief Storage Handle
 */
using Storage = std::uint64_t;

/**
 * @brief the status which is after some function.
 *
 * Warn is no problem for progressing.
 * ERR is problem for progressing.
 */
enum class Status : std::int32_t {
  WARN_ALREADY_DELETE,
  WARN_ALREADY_IN_A_SESSION,
  WARN_ALREADY_INSERT,
  // warning
  /** this warning mean it canceled some previous operation. */
  WARN_CANCEL_PREVIOUS_OPERATION,
  WARN_NOT_FOUND,
  WARN_NOT_IN_A_SESSION,
  // warning
  /** this warning mean it read from local read/write set. */
  WARN_READ_FROM_OWN_OPERATION,
  // warning
  /** WRITE of this warning includes insert/update/upsert */
  WARN_WRITE_TO_LOCAL_WRITE,
  OK,
  ERR_ALREADY_EXISTS,
  // error
  /** It read absent (inserting/deleting) of a version. */
  ERR_ILLEGAL_STATE,
  // error
  /** It is used at leave function. It means that leave function recieved invalid token. */
  ERR_INVALID_ARGS,
  ERR_NOT_FOUND,
  ERR_UNKNOWN,
  // error
  /** read validation failure */
  ERR_VALIDATION,
};

enum class OP_TYPE : std::int32_t {
  SEARCH,
  UPDATE,
  INSERT,
  DELETE,
  UPSERT,
  BEGIN,
  COMMIT,
  ABORT,
};

class Tuple {
public:
  /** key string of db. */
  std::unique_ptr<char[]> key;
  /** val string of db. */
  std::unique_ptr<char[]> val;
  /** length of key string of db. */
  std::size_t len_key;
  /** length of val string of db. */
  std::size_t len_val;

  Tuple() = default;
  ~Tuple() = default;

  Tuple(char const *key, std::size_t len_key, char const *val, std::size_t len_val) {
    this->len_key = len_key;
    this->len_val = len_val;
    this->key = std::make_unique<char[]>(len_key);
    this->val = std::make_unique<char[]>(len_val);
    memcpy(this->key.get(), key, len_key);
    memcpy(this->val.get(), val, len_val);
  }

  Tuple(const Tuple& right) {
    this->len_key = right.len_key;
    this->len_val = right.len_val;
    this->key = std::make_unique<char[]>(right.len_key);
    this->val = std::make_unique<char[]>(right.len_val);
    memcpy(this->key.get(), right.key.get(), right.len_key);
    memcpy(this->val.get(), right.val.get(), right.len_val);
  }

  Tuple& operator=(const Tuple& right) {
    this->len_key = right.len_key;
    this->len_val = right.len_val;
    this->key.reset();
    this->val.reset();
    this->key = std::make_unique<char[]>(right.len_key);
    this->val = std::make_unique<char[]>(right.len_val);
    memcpy(this->key.get(), right.key.get(), right.len_key);
    memcpy(this->val.get(), right.val.get(), right.len_val);
    
    return *this;
  }
};
}  // namespace kvs

