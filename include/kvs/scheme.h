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
   * (delete_all_records) There are no records.
   * (read_from_scan) The read targets was deleted by delete operation of this transaction.
   * (scan_key) The read targets was deleted by delete operation of this transaction.
   * (search_key) The read targets was deleted by delete operation of this transaction.
   */
  WARN_ALREADY_DELETE,
  /**
   * @brief warning
   * @details
   * (insert) The records whose key is the same as @key exists in MTDB, so this function returned immediately.
   */
  WARN_ALREADY_EXISTS,
  /**
   * @brief warning
   * @details
   * (delete_record) it canceled an update/insert operation before this fucntion and did delete operation.
   */
  WARN_CANCEL_PREVIOUS_OPERATION,
  /**
   * @brief warning
   * @details
   * (scan_key) The read targets was deleted by delete operation of concurrent transaction.
   * (search_key) The read targets was deleted by delete operation of concurrent transaction.
   * (read_from_scan) The read targets was deleted by delete operation of concurrent transaction.
   */
  WARN_CONCURRENT_DELETE,
  /**
   * @brief warning
   * @details
   * (close_scan) The handle is invalid.
   * (read_from_scan) The handle is invalid.
   */
  WARN_INVALID_HANDLE,
  /**
   * @brief warning
   * @details
   * (delete_record) No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
   * (open_scan) The scan couldn't find any records.
   * (search_key) No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
   * (update) No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort.
   */
  WARN_NOT_FOUND,
  /**
   * @brief warning
   * @details
   * (leave) If the session is already ended.
   */
  WARN_NOT_IN_A_SESSION,
  /**
   * @brief waring
   * @details 
   * (read_from_scan) It read the records from it's preceding write (insert/update/upsert) operation in the same tx.
   */
  WARN_READ_FROM_OWN_OPERATION,
  /**
   * @brief warning
   * @details 
   * (open_scan) The scan could find some records but could not prese    rve result due to capacity limitation.
   * (read_from_scan) It have read all records in the scan_cache.
   */
  WARN_SCAN_LIMIT,
  /**
   * @brief warning
   * @details 
   * (update) It already executed update/insert, so it up date the value which is going to be updated.
   * (upsert) It already did insert/update/upsert, so it overwrite its local write set.
   */
  WARN_WRITE_TO_LOCAL_WRITE,
  /**
   * @brief success status.
   */
  OK,
  /**
   * @brief error
   * @details
   * (init) The args as a log directory path is invalid.
   */
  ERR_INVALID_ARGS,
  /**
   * @brief error
   * @details
   * (get_storage) If the storage is not registered with the given name.
   * (delete_storage) If the storage is not registered with the given name.
   */
  ERR_NOT_FOUND,
  /**
   * @brief error
   * @details 
   * (enter) There are no capacity of session.
   */
  ERR_SESSION_LIMIT,
  /**
   * @brief error
   * @details
   * (commit) This means read validation failure and it already executed abort(). After this, do tbegin to start next transaction or leave to leave the session.
   */
  ERR_VALIDATION,
  /**
   * @brief error
   * @details 
   * (commit) This transaction was interrupted by some delete transaction between read phase and validation phase. So it called abort.
   */
  ERR_WRITE_TO_DELETED_RECORD,
};

inline constexpr std::string_view to_string_view(Status value) noexcept {
  using namespace std::string_view_literals;
  switch (value) {
    case Status::WARN_ALREADY_DELETE: return "WARN_ALREADY_DELETE"sv;
    case Status::WARN_ALREADY_EXISTS: return "WARN_ALREADY_EXISTS"sv;
    case Status::WARN_CANCEL_PREVIOUS_OPERATION: return "WARN_CANCEL_PREVIOUS_OPERATION"sv;
    case Status::WARN_CONCURRENT_DELETE: return "WARN_CONCURRENT_DELETE"sv;
    case Status::WARN_INVALID_HANDLE: return "WARN_INVALID_HANDLE"sv;
    case Status::WARN_NOT_FOUND: return "WARN_NOT_FOUND"sv;
    case Status::WARN_NOT_IN_A_SESSION: return "WARN_NOT_IN_A_SESSION"sv;
    case Status::WARN_READ_FROM_OWN_OPERATION: return "WARN_READ_FROM_OWN_OPERATION"sv;
    case Status::WARN_SCAN_LIMIT: return "WARN_SCAN_LIMIT"sv;
    case Status::WARN_WRITE_TO_LOCAL_WRITE: return "WARN_WRITE_TO_LOCAL_WRITE"sv;
    case Status::OK: return "OK"sv;
    case Status::ERR_INVALID_ARGS: return "ERR_INVALID_ARGS"sv;
    case Status::ERR_NOT_FOUND: return "ERR_NOT_FOUND"sv;
    case Status::ERR_SESSION_LIMIT: return "ERR_SESSION_LIMIT"sv;
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

