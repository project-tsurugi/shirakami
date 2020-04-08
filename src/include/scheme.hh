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
#include <iostream>
#include <iostream>
#include <tuple>
#include <utility>
#include <vector>

#include "cache_line_size.hh"
#include "compiler.hh"
#include "debug.hh"
#include "fileio.hh"
#include "log.hh"
#include "record.hh"
#include "scheme.hh"
#include "tid.hh"

// kvs_charkey/include/
#include "kvs/scheme.h"

namespace kvs {

/**
 * @brief element of write set.
 * @detail copy constructor/assign operator can't be used in this class 
 * in terms of performance.
 */
class WriteSetObj {
  public:
    WriteSetObj() {}

    // for insert/delete operation
    WriteSetObj(OP_TYPE op, Record* rec_ptr) : op_(op), rec_ptr_(rec_ptr) {}

    // for update/
    WriteSetObj(const char* const key_ptr, const std::size_t key_length, const char* const val_ptr, const std::size_t val_length, const OP_TYPE op, Record* const rec_ptr) : tuple_(key_ptr, key_length, val_ptr, val_length), op_(op), rec_ptr_(rec_ptr) {}

    WriteSetObj(const WriteSetObj& right) = delete;
    // for std::sort
    WriteSetObj(WriteSetObj&& right) = default;
    WriteSetObj& operator=(const WriteSetObj& right) = delete;
    // for std::sort
    WriteSetObj& operator=(WriteSetObj&& right) = default;

    bool operator<(const WriteSetObj& right) const {
      const Tuple* this_tuple_ptr;
      if (this->op_ == OP_TYPE::UPDATE) {
        this_tuple_ptr = this->get_tuple_ptr_to_local();
      } else {
        // insert/delete
        this_tuple_ptr = this->get_tuple_ptr_to_db();
      }
      const Tuple* right_tuple_ptr;
      if (this->op_ == OP_TYPE::UPDATE) {
        right_tuple_ptr = right.get_tuple_ptr_to_local();
      } else {
        // insert/delete
        right_tuple_ptr = right.get_tuple_ptr_to_db();
      }

      const char* this_key_ptr(this_tuple_ptr->get_key().data());
      const char* right_key_ptr(right_tuple_ptr->get_key().data());
      std::size_t this_key_size(this_tuple_ptr->get_key().size());
      std::size_t right_key_size(right_tuple_ptr->get_key().size());

      bool judge = false;
      if (this_key_size < right_key_size) {
        if (memcmp(this_key_ptr, right_key_ptr, this_key_size) <= 0) {
          return true;
        } else {
          return false;
        }
      } else if (this_key_size > right_key_size) {
        if (memcmp(this_key_ptr, right_key_ptr, right_key_size) < 0) {
          return true;
        } else {
          return false;
        }
      } else { // same length
        int ret = memcmp(this_key_ptr, right_key_ptr, this_key_size);      
        if (ret < 0) {
          return true;
        } else if (ret > 0) {
          return false;
        } else {
          ERR; // Unique key is not allowed now.
        }
      }
    }

    Record* get_rec_ptr() ;
    /**
     * @brief get tuple ptr to local write set
     * @details const prohibits overwriting Tuple * entities.
     * @return const Tuple* const
     */
    const Tuple* const get_tuple_ptr_to_local() const;
    /**
     * @brief get tuple ptr to database(global)
     * @details const prohibits overwriting Tuple * entities.
     * @return const Tuple* const
     */
    const Tuple* const get_tuple_ptr_to_db() const;

    const OP_TYPE get_op() { return op_; }

    void reset_tuple(const char* const val_ptr, const std::size_t val_length);

  private:
    /**
     * for update : ptr to existing record.
     * for insert : ptr to new existing record.
     */
    Record* rec_ptr_; // ptr to database
    Tuple tuple_;  // for update
    OP_TYPE op_;
};

class ReadSetObj {
 public:
  Record rec_read;
  Record* rec_ptr; // ptr to database

  ReadSetObj(void) {
    this->rec_ptr = nullptr;
  }

  ReadSetObj(Record* rec_ptr) {
    this->rec_ptr = rec_ptr;
  }

  ReadSetObj(const ReadSetObj& right) = delete;
  ReadSetObj(ReadSetObj&& right) = default;
  ReadSetObj& operator=(const ReadSetObj& right) = delete;
  ReadSetObj& operator=(ReadSetObj&& right) = default;
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
  std::size_t gc_container_index_;
  std::vector<Record*> *gc_container_;

  /**
   * about holding operation info.
   */
  std::vector<ReadSetObj> read_set;
  std::vector<WriteSetObj> write_set;
  std::vector<OprObj> opr_set;

  /**
   * about scan operation.
   */
  std::map<ScanHandle, std::vector<Record*>> scan_cache_;
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
   * Accessor
   */

  /**
   * Getter
   */
  bool get_txbegan_() { return txbegan_; }

  /**
   * @brief clean up about holding operation info.
   */
  void clean_up_ops_set();

  /**
   * @brief clean up about scan operation.
   */
  void clean_up_scan_caches();

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

  /**
   * @brief check whether it already executed search operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  ReadSetObj* search_read_set(const char* key, std::size_t len_key);

  /**
   * @brief check whether it already executed search operation.
   * @param [in] rec_ptr the pointer of record.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  ReadSetObj* search_read_set(Record* rec_ptr);

  /**
   * @brief check whether it already executed write operation.
   * @param [in] key the key of record.
   * @param [in] len_key the key length of records.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  WriteSetObj* search_write_set(const char* key, std::size_t len_key);

  /**
   * @brief check whether it already executed update/insert operation.
   * @param [in] rec_ptr the pointer of record.
   * @return the pointer of element. If it is nullptr, it is not found.
   */
  WriteSetObj* search_write_set(Record* rec_ptr);

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

  Token get_token() const {
    return token_;
  }

  Epoch get_epoch() const {
    return epoch_.load(std::memory_order_acquire);
  }

  TidWord get_mrctid() const {
    return mrctid_;
  }

  bool get_visible() const {
    return visible_.load(std::memory_order_acquire);
  }

  bool get_txbegan() const {
    return txbegan_;
  }

  void set_token(Token token) {
    token_ = token;
  }

  void set_epoch(Epoch epoch) {
    epoch_.store(epoch, std::memory_order_release);
  }

  void set_mrctid(TidWord tid) {
    mrctid_ = tid;
  }

  void set_visible(bool visible) {
    visible_.store(visible, std::memory_order_release);
  }
  
  void set_txbegan(bool txbegan) {
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

