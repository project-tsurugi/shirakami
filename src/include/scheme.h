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
#include <iostream>
#include <vector>

#include "debug.h"

// kvs_charkey/include/
#include "kvs/scheme.h"

namespace kvs {

/**
 * @file
 * @brief private scheme of transaction engine
 */

class TidWord {
public:
  union {
    uint64_t obj;
    struct {
      bool lock:1;
      bool latest:1;
      bool absent:1;
      uint64_t tid:29;
      uint64_t epoch:32;
    };
  };

  TidWord() {
    obj = 0;
  }

  bool operator==(const TidWord& right) const {
    return obj == right.obj;
  }

  bool operator!=(const TidWord& right) const {
    return !operator==(right);
  }

  bool operator<(const TidWord& right) const {
    return this->obj < right.obj;
  }

  bool is_locked() { return lock; }
};

/**
 * @brief element of write set.
 * @detail copy constructor/assign operator can't be used in this class 
 * in terms of performance.
 */
class Record {
public:
  TidWord tidw;
  Tuple tuple;

  Record () {}

  Record(char const *key, std::size_t len_key, char const *val, std::size_t len_val) {
    this->tuple.len_key = len_key;
    this->tuple.len_val = len_val;
    this->tuple.key = std::make_unique<char[]>(len_key);
    this->tuple.val = std::make_unique<char[]>(len_val);
    memcpy(this->tuple.key.get(), key, len_key);
    memcpy(this->tuple.val.get(), val, len_val);
    tuple.visible = false;
  }

  Record(const Record& right) = default;
  Record(Record&& right) = default;
  Record& operator=(const Record& right) = default;
  Record& operator=(Record&& right) = default;

};

/**
 * @brief element of write set.
 * @detail copy constructor/assign operator can't be used in this class 
 * in terms of performance.
 */
class WriteSetObj {
 public:
  //Tuple tuple; // new tuple used ONLY for UPDATE
  std::unique_ptr<char[]> update_val_ptr;
  std::size_t update_len_val;
  OP_TYPE op;
  Record* rec_ptr; // ptr to database

  WriteSetObj() {}

  WriteSetObj(OP_TYPE op, Record* rec_ptr) {
    this->op = op;
    this->rec_ptr = rec_ptr;
  }

  WriteSetObj(char const *val, std::size_t len_val, OP_TYPE op) {
    update_len_val = len_val;
    update_val_ptr = std::make_unique<char[]>(len_val);
    memcpy(update_val_ptr.get(), val, len_val);
    this->op = op;
  }

  WriteSetObj(char const *val, std::size_t len_val, OP_TYPE op, Record* rec_ptr) {
    update_len_val = len_val;
    update_val_ptr = std::make_unique<char[]>(len_val);
    memcpy(update_val_ptr.get(), val, len_val);
    this->op = op;
    this->rec_ptr = rec_ptr;
  }

  WriteSetObj(const WriteSetObj& right) = delete;
  WriteSetObj(WriteSetObj&& right) = default;
  WriteSetObj& operator=(const WriteSetObj& right) = delete;
  WriteSetObj& operator=(WriteSetObj&& right) = default;

  bool operator==(const WriteSetObj& right) const {
    bool judge = false;

    if (rec_ptr->tuple.len_key == right.rec_ptr->tuple.len_key &&
        memcmp(rec_ptr->tuple.key.get(), right.rec_ptr->tuple.key.get(), rec_ptr->tuple.len_key) == 0) {
      judge = true;
    }
    return judge;
  }

  bool operator!=(const WriteSetObj& right) const {
    bool judge = false;

    if (rec_ptr->tuple.len_key != right.rec_ptr->tuple.len_key ||
        memcmp(rec_ptr->tuple.key.get(), right.rec_ptr->tuple.key.get(), rec_ptr->tuple.len_key) != 0) {
      judge = true;
    }

    return judge;
  }

  bool operator<(const WriteSetObj& right) const {
    bool judge = false;
    uint len_this = rec_ptr->tuple.len_key;
    uint len_right = right.rec_ptr->tuple.len_key;

    if (len_this < len_right) {
      int ret = memcmp(rec_ptr->tuple.key.get(), right.rec_ptr->tuple.key.get(), len_this);
      if (ret > 0) judge = false;
      else if (ret <= 0) judge = true;
    }
    else if (len_this > len_right) {
      int ret = memcmp(rec_ptr->tuple.key.get(), right.rec_ptr->tuple.key.get(), len_right);
      if (ret >= 0) judge = false;
      else if (ret < 0) judge = true;
    }
    else { // same length
      int ret = memcmp(rec_ptr->tuple.key.get(), right.rec_ptr->tuple.key.get(), len_right);      
      if (ret > 0) judge = false;
      else if (ret < 0) judge = true;
      else if (ret == 0) {
        //SSS(this->tuple.key);
        //SSS(right.tuple.key);
        ERR; // Unique key is not allowed now.
      }
    }

    return judge;
  }

  void reset(char const *val, std::size_t len_val, OP_TYPE op, Record* rec_ptr) {
    update_len_val = len_val;
    update_val_ptr = std::make_unique<char[]>(len_val);
    memcpy(update_val_ptr.get(), val, len_val);
    this->op = op;
    this->rec_ptr = rec_ptr;
  }
};

class ReadSetObj {
 public:
  Record rec_read;
  Record* rec_ptr; // ptr to database

  ReadSetObj(void) {
    this->rec_ptr = nullptr;
  }

  ReadSetObj(Record* rec_ptr) {
    this->rec_read = *rec_ptr;
    this->rec_ptr = rec_ptr;
  }

  ReadSetObj(const ReadSetObj& right) = delete;
  ReadSetObj(ReadSetObj&& right) = default;
  ReadSetObj& operator=(const ReadSetObj& right) = delete;
  ReadSetObj& operator=(ReadSetObj&& right) = default;
};

class LogBody {
 public:
  uint64_t tidw; // tidword
  Tuple tuple;  
};

class LogShell {
 public:
  uint64_t epoch;
  LogBody *body;
  uint counter;
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
  Token token;
  uint64_t epoch;
  bool visible;
  std::vector<ReadSetObj> read_set;
  std::vector<WriteSetObj> write_set;
  std::vector<OprObj> opr_set;

  ThreadInfo(const Token token) {
    this->token = token;
  }

  ThreadInfo() {
    this->visible = false;
  }

  /**
   * @brief check whether it already executed search operation.
   */
  ReadSetObj* search_read_set(const char* key, std::size_t len_key);

  /**
   * @brief check whether it already executed write operation.
   */
  WriteSetObj* search_write_set(const char* key, std::size_t len_key);
  /**
   * @brief check whether it already executed write operation.
   * @param op identify update or insert.
   */
  WriteSetObj* search_write_set(const char* key, std::size_t len_key, OP_TYPE op);
};

void print_result(struct timeval begin, struct timeval end, int nthread);
void task(int rowid);
void lock(int rowid);
void unlock(int rowid);

}  // namespace kvs

