/**
 * @file scheme.hh
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

#include "cache_line_size.hh"
#include "compiler.hh"
#include "fileio.hh"
#include "log.hh"
#include "record.hh"
#include "scheme.hh"
#include "tid.hh"

// shirakami/include/
#include "kvs/scheme.h"

namespace kvs {

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
  WriteSetObj(WriteSetObj&& right)
      : op_(right.op_), rec_ptr_(right.rec_ptr_), tuple_() {
    tuple_ = std::move(right.tuple_);
  }

  WriteSetObj& operator=(const WriteSetObj& right) = delete;  // NOLINT
  // for std::sort
  WriteSetObj& operator=(WriteSetObj&& right) {  // NOLINT
    rec_ptr_ = right.rec_ptr_;
    op_ = right.op_;
    tuple_ = std::move(right.tuple_);

    return *this;
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

  void reset_tuple_value(const char* val_ptr, std::size_t val_length) &;

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

  explicit ReadSetObj(const Record* const rec_ptr) : rec_read() {
    this->rec_ptr = rec_ptr;
  }

  ReadSetObj(const ReadSetObj& right) = delete;
  ReadSetObj(ReadSetObj&& right) {
    rec_read = std::move(right.rec_read);
    rec_ptr = right.rec_ptr;
  }

  ReadSetObj& operator=(const ReadSetObj& right) = delete;  // NOLINT
  ReadSetObj& operator=(ReadSetObj&& right) {               // NOLINT
    rec_read = std::move(right.rec_read);
    rec_ptr = right.rec_ptr;

    return *this;
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
  Record rec_read;
  const Record* rec_ptr;  // ptr to database
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

class ThreadInfo {
public:
  explicit ThreadInfo(const Token token) {
    this->token_ = token;
    mrctid_.reset();
  }

  ThreadInfo() {
    this->visible_.store(false, std::memory_order_release);
    mrctid_.reset();
    log_dir_.assign(MAC2STR(PROJECT_ROOT));
  }

  /**
   * @brief clean up about holding operation info.
   */
  void clean_up_ops_set();

  /**
   * @brief clean up about scan operation.
   */
  void clean_up_scan_caches();

  /**
   * @brief for debug.
   */
  [[maybe_unused]] void display_read_set();

  [[maybe_unused]] void display_write_set();

  /**
   * @brief check whether it already executed update or insert operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @pre this function is only executed in delete_record operation.
   * @return Status::OK no update/insert before this delete_record operation.
   * @return Status::WARN_CANCEL_PREVIOUS_OPERATION it canceled an update/insert
   * operation before this delete_record operation.
   */
  Status check_delete_after_write(const char* key,  // NOLINT
                                  std::size_t len_key);

  /**
   * @brief Remove inserted records of write set from masstree.
   *
   * Insert operation inserts records to masstree in read phase.
   * If the transaction is aborted, the records exists for ever with absent
   * state. So it needs to remove the inserted records of write set from
   * masstree at abort.
   * @pre This function is called at abort.
   */
  void remove_inserted_records_of_write_set_from_masstree();

  void gc_records_and_values() const;

  /**
   * @brief check whether it already executed search operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  ReadSetObj* search_read_set(const char* key, std::size_t len_key);  // NOLINT

  /**
   * @brief check whether it already executed search operation.
   * @param [in] rec_ptr the pointer of record.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  ReadSetObj* search_read_set(const Record* rec_ptr);  // NOLINT

  /**
   * @brief check whether it already executed write operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  WriteSetObj* search_write_set(const char* key,  // NOLINT
                                std::size_t len_key);

  /**
   * @brief check whether it already executed update/insert operation.
   * @param [in] rec_ptr the pointer of record.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  const WriteSetObj* search_write_set(const Record* rec_ptr);  // NOLINT

#if 0
  /**
   * @brief check whether it already executed write operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @param [in] op identify update or insert.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  [[maybe_unused]] WriteSetObj* search_write_set(const char* key,      // NOLINT
                                                 std::size_t len_key,  // NOLINT
                                                 OP_TYPE op);          // NOLINT
#endif

  /**
   * @brief unlock records in write set.
   *
   * This function unlocked all records in write set absolutely.
   * So it has a pre-condition.
   * @pre It has locked all records in write set.
   * @return void
   */
  void unlock_write_set();

  /**
   * @brief unlock write set object between @a begin and @a end.
   * @param [in] begin Starting points.
   * @param [in] end Ending points.
   * @pre It already locked write set between @a begin and @a end.
   * @return void
   */
  void unlock_write_set(std::vector<WriteSetObj>::iterator begin,
                        std::vector<WriteSetObj>::iterator end);

  /**
   * @brief write-ahead logging
   * @param [in] ctid commit tid.
   * @return void
   */
  void wal(uint64_t ctid);

  /**
   * CAS
   */

  bool cas_visible(bool& expected, bool& desired) & {  // NOLINT
    return visible_.compare_exchange_strong(expected, desired,
                                            std::memory_order_acq_rel);
  }

  /**
   * Getter
   */

  [[nodiscard]] Epoch get_epoch() const& {  // NOLINT
    return epoch_.load(std::memory_order_acquire);
  }

  [[maybe_unused]] std::size_t get_gc_container_index() {  // NOLINT
    return gc_container_index_;
  }

  std::vector<Record*>* get_gc_record_container() {  // NOLINT
    return gc_record_container_;
  }

  std::vector<std::pair<std::string*, Epoch>>*
  get_gc_value_container() {  // NOLINT
    return gc_value_container_;
  }

  std::map<ScanHandle, std::size_t>& get_len_rkey() {  // NOLINT
    return len_rkey_;
  }

  std::vector<Log::LogRecord>& get_log_set() {  // NOLINT
    return log_set_;
  }

  TidWord& get_mrctid() & { return mrctid_; }  // NOLINT

  [[maybe_unused]] [[nodiscard]] const TidWord& get_mrctid() const& {  // NOLINT
    return mrctid_;
  }

  std::vector<OprObj>& get_opr_set() {  // NOLINT
    return opr_set;
  }

  std::map<ScanHandle, bool>& get_r_exclusive() {  // NOLINT
    return r_exclusive_;
  }

  std::map<ScanHandle, std::unique_ptr<char[]>>& get_rkey() {  // NOLINT
    return rkey_;
  }

  std::vector<ReadSetObj>& get_read_set() {  // NOLINT
    return read_set;
  }

  std::map<ScanHandle, std::vector<const Record*>>&
  get_scan_cache() {  // NOLINT
    return scan_cache_;
  }

  std::map<ScanHandle, std::size_t>& get_scan_cache_itr() {  // NOLINT
    return scan_cache_itr_;
  }

  [[maybe_unused]] Token& get_token() & { return token_; }  // NOLINT

  [[maybe_unused]] [[nodiscard]] const Token& get_token() const& {  // NOLINT
    return token_;
  }

  bool& get_txbegan() & { return txbegan_; }  // NOLINT

  [[maybe_unused]] [[nodiscard]] const bool& get_txbegan() const& {
    return txbegan_;
  }  // NOLINT

  [[nodiscard]] bool get_visible() const& {  // NOLINT
    return visible_.load(std::memory_order_acquire);
  }

  std::vector<WriteSetObj>& get_write_set() {  // NOLINT
    return write_set;
  }

  /**
   * setter
   */

  [[maybe_unused]] void set_token(Token token) & { token_ = token; }

  void set_epoch(Epoch epoch) & {
    epoch_.store(epoch, std::memory_order_release);
  }

  void set_gc_container_index(std::size_t new_index) {
    gc_container_index_ = new_index;
  }

  void set_gc_record_container(std::vector<Record*>* new_cont) {  // NOLINT
    gc_record_container_ = new_cont;
  }

  void set_gc_value_container(  // NOLINT
      std::vector<std::pair<std::string*, Epoch>>* new_cont) {
    gc_value_container_ = new_cont;
  }

  void set_mrctid(const TidWord& tid) & { mrctid_ = tid; }

  void set_visible(bool visible) & {
    visible_.store(visible, std::memory_order_release);
  }

  void set_txbegan(bool txbegan) & { txbegan_ = txbegan; }

private:
  alignas(CACHE_LINE_SIZE) Token token_{};
  std::atomic<Epoch> epoch_{};
  TidWord mrctid_{};  // most recently chosen tid, for calculate new tids.
  std::atomic<bool> visible_{};
  bool txbegan_{};

  /**
   * about garbage collection
   */
  std::size_t gc_container_index_{};  // common to record and value;
  std::vector<Record*>* gc_record_container_{};
  std::vector<std::pair<std::string*, Epoch>>* gc_value_container_{};

  /**
   * about holding operation info.
   */
  std::vector<ReadSetObj> read_set{};
  std::vector<WriteSetObj> write_set{};
  std::vector<OprObj> opr_set{};

  /**
   * about scan operation.
   */
  std::map<ScanHandle, std::vector<const Record*>> scan_cache_{};
  std::map<ScanHandle, std::size_t> scan_cache_itr_{};
  std::map<ScanHandle, std::unique_ptr<char[]>> rkey_{};  // NOLINT
  std::map<ScanHandle, std::size_t> len_rkey_{};
  std::map<ScanHandle, bool> r_exclusive_{};
  /**
   * about logging.
   */
  std::string log_dir_{};
  File logfile_{};
  Log::LogHeader latest_log_header_{};
  std::vector<Log::LogRecord> log_set_{};
};

[[maybe_unused]] extern void print_result(struct timeval begin,
                                          struct timeval end, int nthread);
[[maybe_unused]] extern void print_status(Status status);

}  // namespace kvs
