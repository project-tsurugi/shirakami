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
using namespace std;

typedef enum {
  SEARCH, UPDATE, INSERT, DELETE, UPSERT,
  BEGIN, COMMIT, ABORT} OP_TYPE;

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
};

class Tuple {
public:
  char *key = nullptr;
  char *val = nullptr;
  uint len_key;
  uint len_val;
  bool visible; // for delete, search

  Tuple() {
    this->visible = true;
  }

  Tuple(char *key, const uint len_key, char *val, const uint len_val) {
    this->len_key = len_key;
    this->len_val = len_val;
    this->visible = true;
    this->key = (char *)calloc(1, len_key); if (!this->key) ERR;
    this->val = (char *)calloc(1, len_val); if (!this->val) ERR;
    memcpy(this->key, key, len_key);
    memcpy(this->val, val, len_val);
  }
};

class Record {
public:
  TidWord tidw;
  Tuple tuple;

	Record () {
	}

	Record(char *key, uint len_key, char *val, uint len_val) {
		this->tuple.len_key = len_key;
		this->tuple.len_val = len_val;
		if (!(this->tuple.key = (char *)calloc(len_key, sizeof(char)))) ERR;
		if (!(this->tuple.val = (char *)calloc(len_val, sizeof(char)))) ERR;
		memcpy(this->tuple.key, key, len_key);
		memcpy(this->tuple.val, val, len_val);
  }

  ~Record () {
  }
};

class WriteSetObj {
 public:
  //Tuple tuple; // new tuple used ONLY for UPDATE
	char *update_val;
	uint update_len_val;
  Record* rec_ptr; // ptr to database
  OP_TYPE op;

	bool operator==(const WriteSetObj& right) const {
    bool judge = false;

    if (rec_ptr->tuple.len_key == right.rec_ptr->tuple.len_key &&
				memcmp(rec_ptr->tuple.key, right.rec_ptr->tuple.key, rec_ptr->tuple.len_key) == 0) {
      judge = true;
    }
		return judge;
	}

	bool operator!=(const WriteSetObj& right) const {
    bool judge = false;

		if (rec_ptr->tuple.len_key != right.rec_ptr->tuple.len_key ||
				memcmp(rec_ptr->tuple.key, right.rec_ptr->tuple.key, rec_ptr->tuple.len_key) != 0) {
      judge = true;
    }

		return judge;
	}

	bool operator<(const WriteSetObj& right) const {
    bool judge = false;
    uint len_this = rec_ptr->tuple.len_key;
    uint len_right = right.rec_ptr->tuple.len_key;

    if (len_this < len_right) {
      int ret = memcmp(rec_ptr->tuple.key, right.rec_ptr->tuple.key, len_this);
      if (ret > 0) judge = false;
      else if (ret <= 0) judge = true;
    }
    else if (len_this > len_right) {
      int ret = memcmp(rec_ptr->tuple.key, right.rec_ptr->tuple.key, len_right);
      if (ret >= 0) judge = false;
      else if (ret < 0) judge = true;
    }
    else { // same length
      int ret = memcmp(rec_ptr->tuple.key, right.rec_ptr->tuple.key, len_right);      
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
};

class ReadSetObj {
 public:
  Record rec_read;
	Record* rec_ptr; // ptr to database

	ReadSetObj(void) {
		this->rec_ptr = nullptr;
	}
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
  char *key;
  char *value;

  OprObj() {
    this->key = nullptr;
    this->value = nullptr;
  }

  OprObj(char *key, char *value, OP_TYPE type) {
    this->key = key;
    this->value = value;
    this->type = type;
  }
}; 

class ThreadInfo {
 public:
  uint token;
  uint64_t epoch;
	bool visible;
  std::vector<ReadSetObj> readSet;
  std::vector<WriteSetObj> writeSet;
  std::vector<OprObj> oprSet;

  ThreadInfo(const uint token) {
    this->token = token;
  }

	ThreadInfo() {
    this->visible = false;
  }
};

void print_result(struct timeval begin, struct timeval end, int nthread);
void task(int rowid);
void lock(int rowid);
void unlock(int rowid);
