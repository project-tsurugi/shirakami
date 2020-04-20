/**
 * @file
 * @brief private scheme of transaction engine
 */

#pragma once

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
 * @detail copy constructor/assign operator can't be used in this class 
 * in terms of performance.
 */
class WriteSetObj {
  public:
    WriteSetObj() : tuple_() {}

    // for insert/delete operation
    WriteSetObj(OP_TYPE op, Record* rec_ptr) : op_(op), rec_ptr_(rec_ptr) {}

    // for update/
    WriteSetObj(const char* const key_ptr, const std::size_t key_length, const char* const val_ptr, const std::size_t val_length, const OP_TYPE op, Record* const rec_ptr) : op_(op), rec_ptr_(rec_ptr), tuple_(key_ptr, key_length, val_ptr, val_length) {}

    WriteSetObj(const WriteSetObj& right) = delete;
    // for std::sort
    WriteSetObj(WriteSetObj&& right) : op_(right.op_), rec_ptr_(right.rec_ptr_), tuple_() {
      tuple_ = std::move(right.tuple_);
    }

    WriteSetObj& operator=(const WriteSetObj& right) = delete;
    // for std::sort
    WriteSetObj& operator=(WriteSetObj&& right) {
      rec_ptr_ = right.rec_ptr_;
      op_ = right.op_;
      tuple_ = std::move(right.tuple_);

      return *this;
    }

    bool operator<(const WriteSetObj& right) const;

    Record* get_rec_ptr() & {
      return this->rec_ptr_;
    }

    const Record* get_rec_ptr() const & {
      return this->rec_ptr_;
    }

    /**
     * @brief get tuple ptr appropriately by operation type.
     * @return Tuple& 
     */
    Tuple& get_tuple() & {
      return get_tuple(op_);
    }

    const Tuple& get_tuple() const & {
      return get_tuple(op_);
    }

    /**
     * @brief get tuple ptr appropriately by operation type.
     * @return Tuple& 
     */
    Tuple& get_tuple(const OP_TYPE op) & {
      if (op == OP_TYPE::UPDATE) {
        return get_tuple_to_local();
      } else {
        // insert/delete
        return get_tuple_to_db();
      }
    }

    /**
     * @brief get tuple ptr appropriately by operation type.
     * @return const Tuple& const
     */
    const Tuple& get_tuple(const OP_TYPE op) const & {
      if (op == OP_TYPE::UPDATE) {
        return get_tuple_to_local();
      } else {
        // insert/delete
        return get_tuple_to_db();
      }
    }

    /**
     * @brief get tuple ptr to local write set
     * @return Tuple& 
     */
    Tuple& get_tuple_to_local() & {
      return this->tuple_;
    }

    /**
     * @brief get tuple ptr to local write set
     * @return const Tuple& 
     */
    const Tuple& get_tuple_to_local() const & {
      return this->tuple_;
    }

    /**
     * @brief get tuple ptr to database(global)
     * @return Tuple& 
     */
    Tuple& get_tuple_to_db() & {
      return this->rec_ptr_->get_tuple();
    }

    /**
     * @brief get tuple ptr to database(global)
     * @return const Tuple& 
     */
    const Tuple& get_tuple_to_db() const & {
      return this->rec_ptr_->get_tuple();
    }

    OP_TYPE& get_op() & { 
      return op_; 
    }

    const OP_TYPE& get_op() const & { 
      return op_; 
    }

    void reset_tuple_value(const char* const val_ptr, const std::size_t val_length)&;

  private:
    /**
     * for update : ptr to existing record.
     * for insert : ptr to new existing record.
     */
    OP_TYPE op_;
    Record* rec_ptr_; // ptr to database
    Tuple tuple_;  // for update
};

class ReadSetObj {
public:
  ReadSetObj(void) {
    this->rec_ptr = nullptr;
  }

  ReadSetObj(const Record* const rec_ptr) : rec_read() {
    this->rec_ptr = rec_ptr;
  }

  ReadSetObj(const ReadSetObj& right) = delete;
  ReadSetObj(ReadSetObj&& right) {
    rec_read = std::move(right.rec_read);
    rec_ptr = right.rec_ptr;
  }

  ReadSetObj& operator=(const ReadSetObj& right) = delete;
  ReadSetObj& operator=(ReadSetObj&& right) {
    rec_read = std::move(right.rec_read);
    rec_ptr = right.rec_ptr;
    
    return *this;
  }


  Record& get_rec_read() { return rec_read; }

  const Record& get_rec_read() const { return rec_read; }

  const Record* get_rec_ptr() { return rec_ptr; }

  const Record* get_rec_ptr() const { return rec_ptr; }

private:
  Record rec_read;
  const Record* rec_ptr; // ptr to database

};

class OprObj { // Operations for retry by abort
public:
  OP_TYPE type;
  std::unique_ptr<char[]> key;
  std::unique_ptr<char[]> val;
  std::size_t len_key;
  std::size_t len_val;

  OprObj() = default;
  ~OprObj() = default;

  OprObj(OP_TYPE type, char const *key, std::size_t len_key) {
    this->type = type;
    this->len_key = len_key;
    this->key = std::make_unique<char[]>(len_key);
    memcpy(this->key.get(), key, len_key);
  }

  OprObj(OP_TYPE type, char const *key, std::size_t len_key, char const *val, std::size_t len_val) {
    this->type = type;
    this->len_key = len_key;
    this->len_val = len_val;
    this->key = std::make_unique<char[]>(len_key);
    this->val = std::make_unique<char[]>(len_val);
    memcpy(this->key.get(), key, len_key);
    memcpy(this->val.get(), val, len_val);
  }

  OprObj(const OprObj& right) = delete;
  OprObj(OprObj&& right) = default;
  OprObj& operator=(const OprObj& right) = delete;
  OprObj& operator=(OprObj&& right) = default;
}; 

class ThreadInfo {
 public:
  /**
   * about garbage collection
   */
  std::size_t gc_container_index_; // common to record and value;
  std::vector<Record*> *gc_record_container_;
  std::vector<std::pair<std::string*, Epoch>> *gc_value_container_;

  /**
   * about holding operation info.
   */
  std::vector<ReadSetObj> read_set;
  std::vector<WriteSetObj> write_set;
  std::vector<OprObj> opr_set;

  /**
   * about scan operation.
   */
  std::map<ScanHandle, std::vector<const Record*>> scan_cache_;
  std::map<ScanHandle, std::size_t> scan_cache_itr_;
  std::map<ScanHandle, std::unique_ptr<char[]>> rkey_;
  std::map<ScanHandle, std::size_t> len_rkey_;
  std::map<ScanHandle, bool> r_exclusive_;
  /**
   * about logging.
   */
  std::string log_dir_;
  File logfile_;
  LogHeader latest_log_header_;
  std::vector<LogRecord> log_set_;

  ThreadInfo(const Token token) {
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
  void display_read_set();

  void display_write_set();

  /**
   * @brief check whether it already executed update or insert operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @pre this function is only executed in delete_record operation.
   * @return Status::OK no update/insert before this delete_record operation.
   * @return Status::WARN_CANCEL_PREVIOUS_OPERATION it canceled an update/insert operation before this delete_record operation.
   */
  Status check_delete_after_write(const char* key, const std::size_t len_key);

  /**
   * @brief Remove inserted records of write set from masstree.
   *
   * Insert operation inserts records to masstree in read phase. 
   * If the transaction is aborted, the records exists for ever with absent state.
   * So it needs to remove the inserted records of write set from masstree at abort.
   * @pre This function is called at abort.
   */
  void remove_inserted_records_of_write_set_from_masstree();

  void gc_records_and_values();

  /**
   * @brief check whether it already executed search operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  ReadSetObj* search_read_set(const char* const key, const std::size_t len_key);

  /**
   * @brief check whether it already executed search operation.
   * @param [in] rec_ptr the pointer of record.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  ReadSetObj* search_read_set(const Record* const rec_ptr);

  /**
   * @brief check whether it already executed write operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  WriteSetObj* search_write_set(const char* key, const std::size_t len_key);

  /**
   * @brief check whether it already executed update/insert operation.
   * @param [in] rec_ptr the pointer of record.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  const WriteSetObj* search_write_set (const Record* const rec_ptr);

  /**
   * @brief check whether it already executed write operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @param [in] op identify update or insert.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  WriteSetObj* search_write_set(const char* key, std::size_t len_key, OP_TYPE op);

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
  void unlock_write_set(std::vector<WriteSetObj>::iterator begin, std::vector<WriteSetObj>::iterator end);

  /**
   * @brief write-ahead logging
   * @param [in] ctid commit tid.
   * @return void
   */
  void wal(uint64_t ctid);

  /**
   * CAS
   */

  bool cas_visible(bool& expected, bool& desired) & {
    return visible_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel);
  }

  /**
   * Getter
   */

  Token& get_token() & {
    return token_;
  }

  const Token& get_token() const & {
    return token_;
  }

  Epoch get_epoch() const & {
    return epoch_.load(std::memory_order_acquire);
  }

  TidWord& get_mrctid() & {
    return mrctid_;
  }

  const TidWord& get_mrctid() const & {
    return mrctid_;
  }

  bool get_visible() const & {
    return visible_.load(std::memory_order_acquire);
  }

  bool& get_txbegan() & {
    return txbegan_;
  }

  const bool& get_txbegan() const & {
    return txbegan_;
  }

  /**
   * Accessor
   */

  void set_token(Token token) & {
    token_ = token;
  }

  void set_epoch(Epoch epoch) & {
    epoch_.store(epoch, std::memory_order_release);
  }

  void set_mrctid(TidWord tid) & {
    mrctid_ = tid;
  }

  void set_visible(bool visible) & {
    visible_.store(visible, std::memory_order_release);
  }
  
  void set_txbegan(bool txbegan) & {
    txbegan_ = txbegan;
  }

private:
  alignas(CACHE_LINE_SIZE)
    Token token_;
  std::atomic<Epoch> epoch_;
  TidWord mrctid_; // most recently chosen tid, for calculate new tids.
  std::atomic<bool> visible_;
  bool txbegan_;
};

extern void print_result(struct timeval begin, struct timeval end, int nthread);
extern void print_status(Status status);

}  // namespace kvs

