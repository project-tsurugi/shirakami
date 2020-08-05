/**
 * @file cc/silo_variant/include/scheme.h
 * @brief private scheme of transaction engine
 */

#pragma once

#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "compiler.h"
#include "cpu.h"
#include "fileio.h"
#include "kvs/scheme.h"
#include "log.h"
#include "record.h"
#include "scheme.h"
#include "tid.h"

#ifdef INDEX_YAKUSHIMA
#include "yakushima/include/kvs.h"
#endif

namespace shirakami::cc_silo_variant {

/**
 * @brief element of write set.
 * @details copy constructor/assign operator can't be used in this class
 * in terms of performance.
 */
class WriteSetObj {  // NOLINT
public:
  // for insert/delete operation
  WriteSetObj(OP_TYPE op, Record* rec_ptr) : op_(op), rec_ptr_(rec_ptr) {}

  // for update/
  WriteSetObj(const char* const key_ptr, const std::size_t key_length,
              const char* const val_ptr, const std::size_t val_length,
              const OP_TYPE op, Record* const rec_ptr)
      : op_(op),
        rec_ptr_(rec_ptr),
        tuple_(key_ptr, key_length, val_ptr, val_length) {}

  WriteSetObj(const WriteSetObj& right) = delete;
  // for std::sort
  WriteSetObj(WriteSetObj&& right) : op_(right.op_), rec_ptr_(right.rec_ptr_) {
    tuple_ = std::move(right.tuple_);
  }

  WriteSetObj& operator=(const WriteSetObj& right) = delete;  // NOLINT
  // for std::sort
  WriteSetObj& operator=(WriteSetObj&& right) {  // NOLINT
    rec_ptr_ = right.rec_ptr_;  // It must copy pointer. It can't use default
                                // move assign operator.
    op_ = right.op_;
    tuple_ = std::move(right.tuple_);

    return *this;  // NOLINT
  }

  bool operator<(const WriteSetObj& right) const;  // NOLINT

  Record* get_rec_ptr() & { return this->rec_ptr_; }  // NOLINT

  [[maybe_unused]] [[nodiscard]] const Record* get_rec_ptr() const& {  // NOLINT
    return this->rec_ptr_;
  }

  /**
   * @brief get tuple ptr appropriately by operation type.
   * @return Tuple&
   */
  Tuple& get_tuple() & { return get_tuple(op_); }  // NOLINT

  [[maybe_unused]] [[nodiscard]] const Tuple& get_tuple() const& {  // NOLINT
    return get_tuple(op_);
  }

  /**
   * @brief get tuple ptr appropriately by operation type.
   * @return Tuple&
   */
  Tuple& get_tuple(const OP_TYPE op) & {  // NOLINT
    if (op == OP_TYPE::UPDATE) {
      return get_tuple_to_local();
    }
    // insert/delete
    return get_tuple_to_db();
  }

  /**
   * @brief get tuple ptr appropriately by operation type.
   * @return const Tuple& const
   */
  [[nodiscard]] const Tuple& get_tuple(const OP_TYPE op) const& {  // NOLINT
    if (op == OP_TYPE::UPDATE) {
      return get_tuple_to_local();
    }
    // insert/delete
    return get_tuple_to_db();
  }

  /**
   * @brief get tuple ptr to local write set
   * @return Tuple&
   */
  Tuple& get_tuple_to_local() & { return this->tuple_; }  // NOLINT

  /**
   * @brief get tuple ptr to local write set
   * @return const Tuple&
   */
  [[nodiscard]] const Tuple& get_tuple_to_local() const& {  // NOLINT
    return this->tuple_;
  }

  /**
   * @brief get tuple ptr to database(global)
   * @return Tuple&
   */
  Tuple& get_tuple_to_db() & { return this->rec_ptr_->get_tuple(); }  // NOLINT

  /**
   * @brief get tuple ptr to database(global)
   * @return const Tuple&
   */
  [[nodiscard]] const Tuple& get_tuple_to_db() const& {  // NOLINT
    return this->rec_ptr_->get_tuple();
  }

  OP_TYPE& get_op() & { return op_; }  // NOLINT

  [[nodiscard]] const OP_TYPE& get_op() const& { return op_; }  // NOLINT

  void reset_tuple_value(std::string_view val);

private:
  /**
   * for update : ptr to existing record.
   * for insert : ptr to new existing record.
   */
  OP_TYPE op_;
  Record* rec_ptr_;  // ptr to database
  Tuple tuple_;      // for update
};

class ReadSetObj {  // NOLINT
public:
  ReadSetObj() { this->rec_ptr = nullptr; }

#ifdef INDEX_YAKUSHIMA
  explicit ReadSetObj(const Record* rec_ptr, bool scan = false,
                      yakushima::node_version64_body nvb = {},
                      yakushima::node_version64* nv_ptr = nullptr)  // NOLINT
      : is_scan{scan} {
    this->rec_ptr = rec_ptr;
    nv = std::make_pair(nvb, nv_ptr);
  }
#elif
  explicit ReadSetObj(const Record* rec_ptr, bool scan = false)  // NOLINT
      : is_scan{scan} {
    this->rec_ptr = rec_ptr;
  }
#endif

  ReadSetObj(const ReadSetObj& right) = delete;

  ReadSetObj(ReadSetObj&& right, bool scan = false) : is_scan{scan} {  // NOLINT
    rec_read = std::move(right.rec_read);
    rec_ptr = right.rec_ptr;
  }

  ReadSetObj& operator=(const ReadSetObj& right) = delete;  // NOLINT
  ReadSetObj& operator=(ReadSetObj&& right) {               // NOLINT
    rec_read = std::move(right.rec_read);
    rec_ptr = right.rec_ptr;

    return *this;
  }

  [[nodiscard]] bool get_is_scan() const { return is_scan; }  // NOLINT

  std::pair<yakushima::node_version64_body, yakushima::node_version64*>
  get_nv() {  // NOLINT
    return nv;
  }

  Record& get_rec_read() { return rec_read; }  // NOLINT

  [[nodiscard]] const Record& get_rec_read() const {  // NOLINT
    return rec_read;
  }

  const Record* get_rec_ptr() { return rec_ptr; }  // NOLINT

  [[maybe_unused]] [[nodiscard]] const Record* get_rec_ptr() const {  // NOLINT
    return rec_ptr;
  }

private:
  Record rec_read{};
  const Record* rec_ptr{};  // ptr to database
  bool is_scan{false};      // NOLINT
#ifdef INDEX_YAKUSHIMA
  std::pair<yakushima::node_version64_body, yakushima::node_version64*> nv{};
#endif
};

// Operations for retry by abort
class OprObj {  // NOLINT
public:
  OprObj() = default;
  OprObj(const OP_TYPE type, const char* key_ptr,            // NOLINT
         const std::size_t key_length)                       // NOLINT
      : type_(type), key_(key_ptr, key_length), value_() {}  // NOLINT
  OprObj(const OP_TYPE type, const char* key_ptr,            // NOLINT
         const std::size_t key_length, const char* value_ptr,
         const std::size_t value_length)
      : type_(type),                        // NOLINT
        key_(key_ptr, key_length),          // NOLINT
        value_(value_ptr, value_length) {}  // NOLINT

  OprObj(const OprObj& right) = delete;
  OprObj(OprObj&& right) = default;
  OprObj& operator=(const OprObj& right) = delete;  // NOLINT
  OprObj& operator=(OprObj&& right) = default;      // NOLINT

  ~OprObj() = default;

  OP_TYPE get_type() & { return type_; }  // NOLINT
  std::string_view get_key() & {          // NOLINT
    return {key_.data(), key_.size()};
  }
  std::string_view get_value() & {  // NOLINT
    return {value_.data(), value_.size()};
  }

private:
  OP_TYPE type_{};
  std::string key_{};
  std::string value_{};
};

}  // namespace shirakami::cc_silo_variant
