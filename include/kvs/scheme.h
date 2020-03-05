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
 * @brief Scan Handle
 */
using ScanHandle = std::size_t;

/**
 * @brief the status which is after some function.
 *
 * Warn is no problem for progressing.
 * ERR is problem for progressing.
 */
enum class Status : std::int32_t {
  /**
   * @brief warning
   * @details
   * (read_record) the read record was deleted.
   */
  WARN_ALREADY_DELETE,
  WARN_ALREADY_EXISTS,
  WARN_ALREADY_IN_A_SESSION,
  WARN_ALREADY_INSERT,
  /**
   * @brief warning
   * @details This warning mean it canceled some previous operation.
   */
  WARN_CANCEL_PREVIOUS_OPERATION,
  /**
   * @brief error about invalid handle
   * @details
   * (read_from_scan) it is called when read_from_scan is called with invalid handles.
   */
  WARN_INVALID_HANDLE,
  WARN_CONCURRENT_DELETE,
  WARN_NOT_FOUND,
  WARN_NOT_IN_A_SESSION,
  /**
   * @brief waring
   * @details This warning mean it read from local read/write set.
   */
  WARN_READ_FROM_OWN_OPERATION,
  /**
   * @brief warning
   * @details 
   * (open_scan) the session did open_scan SIZE_MAX times.
   * (read_from_scan) no rest in scan cache.
   */
  WARN_SCAN_LIMIT,
  /**
   * @brief warning
   * @details
   * (delete_all_records) This function was interrupted by someone and did not finish completely.
   */
  WARN_UNKNOWN, 
  // warning
  /**
   * @brief warning
   * @details WRITE of this warning includes insert/update/upsert.
   */
  WARN_WRITE_TO_LOCAL_WRITE,
  OK,
  ERR_ALREADY_EXISTS,
  // error
  /**
   * @brief error
   * @details It read absent (inserting/deleting) of a version.
   */
  ERR_ILLEGAL_STATE,
  // error
  /**
   * @brief error
   * @details It is used at leave function. It means that leave function recieved invalid token.
   */
  ERR_INVALID_ARGS,
  /**
   * @brief error about invalid handle
   * @details
   * (read_from_scan) it is called when read_from_scan is called with invalid handles.
   */
  ERR_INVALID_HANDLE,
  ERR_NOT_FOUND,
  /**
   * @brief error
   * @pre It did enter function.
   * @details It did enter function, however, the maxmum number of session is working. So it can't get session.
   * @post Try enter till success.
   */
  ERR_SESSION_LIMIT,
  ERR_UNKNOWN,
  /**
   * @brief error
   * @details read validation failure.
   */
  ERR_VALIDATION,
  /**
   * @brief error
   * @details write to deleted record.
   */
  ERR_WRITE_TO_DELETED_RECORD,
};

inline constexpr std::string_view to_string_view(Status value) noexcept {
  using namespace std::string_view_literals;
  switch (value) {
    case Status::WARN_ALREADY_DELETE: return "WARN_ALREADY_DELETE"sv;
    case Status::WARN_ALREADY_EXISTS: return "WARN_ALREADY_EXISTS"sv;
    case Status::WARN_ALREADY_IN_A_SESSION: return "WARN_ALREADY_IN_A_SESSION"sv;
    case Status::WARN_ALREADY_INSERT: return "WARN_ALREADY_INSERT"sv;
    case Status::WARN_CANCEL_PREVIOUS_OPERATION: return "WARN_CANCEL_PREVIOUS_OPERATION"sv;
    case Status::WARN_CONCURRENT_DELETE: return "WARN_CONCURRENT_DELETE"sv;
    case Status::WARN_INVALID_HANDLE: return "WARN_INVALID_HANDLE"sv;
    case Status::WARN_NOT_FOUND: return "WARN_NOT_FOUND"sv;
    case Status::WARN_NOT_IN_A_SESSION: return "WARN_NOT_IN_A_SESSION"sv;
    case Status::WARN_READ_FROM_OWN_OPERATION: return "WARN_READ_FROM_OWN_OPERATION"sv;
    case Status::WARN_SCAN_LIMIT: return "WARN_SCAN_LIMIT"sv;
    case Status::WARN_UNKNOWN: return "WARN_UNKNOWN"sv;
    case Status::WARN_WRITE_TO_LOCAL_WRITE: return "WARN_WRITE_TO_LOCAL_WRITE"sv;
    case Status::OK: return "OK"sv;
    case Status::ERR_ALREADY_EXISTS: return "ERR_ALREADY_EXISTS"sv;
    case Status::ERR_ILLEGAL_STATE: return "ERR_ILLEGAL_STATE"sv;
    case Status::ERR_INVALID_ARGS: return "ERR_INVALID_ARGS"sv;
    case Status::ERR_INVALID_HANDLE: return "ERR_INVALID_HANDLE"sv;
    case Status::ERR_NOT_FOUND: return "ERR_NOT_FOUND"sv;
    case Status::ERR_SESSION_LIMIT: return "ERR_SESSION_LIMIT"sv;
    case Status::ERR_UNKNOWN: return "ERR_UNKNOWN"sv;
    case Status::ERR_VALIDATION: return "ERR_VALIDATION"sv;
    case Status::ERR_WRITE_TO_DELETED_RECORD: return "ERR_WRITE_TO_DELETED_RECORD"sv;
  }
  std::abort();
}

inline std::ostream& operator<<(std::ostream& out, Status value) {
    return out << to_string_view(value);
}

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
  /** length of key string of db. */
  std::size_t len_key;
  /** length of val string of db. */
  std::size_t len_val;
  /** key string of db. */
  std::unique_ptr<char[]> key;
  /** val string of db. */
  std::unique_ptr<char[]> val;

  Tuple() = default;
  ~Tuple() = default;

  Tuple(const char* const key, const std::size_t len_key, const char* const val, const std::size_t len_val) {
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
    if (right.len_key > 0) {
      this->key = std::make_unique<char[]>(right.len_key);
      memcpy(this->key.get(), right.key.get(), right.len_key);
    }
    if (right.len_val > 0) {
      this->val = std::make_unique<char[]>(right.len_val);
      memcpy(this->val.get(), right.val.get(), right.len_val);
    }
  }

  Tuple(Tuple&& right) {
    this->len_key = right.len_key;
    this->len_val = right.len_val;
    this->key = std::move(right.key);
    this->val = std::move(right.val);
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

  Tuple& operator=(Tuple&& right) {
    this->len_key = right.len_key;
    this->len_val = right.len_val;
    this->key = std::move(right.key);
    right.key.reset();
    this->val = std::move(right.val);
    right.val.reset();
  }
};
}  // namespace kvs

