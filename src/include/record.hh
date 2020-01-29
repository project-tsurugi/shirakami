/**
 * @file header about record
 * @brief utilities about record
 */

#pragma once

#include "tid.hh"
#include "kvs/scheme.h"

namespace kvs {
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

    tidw = TidWord();
    tidw.absent = true;
  }

  Record(const Record& right) = default;
  Record(Record&& right) = default;
  Record& operator=(const Record& right) = default;
  Record& operator=(Record&& right) = default;

};
} // namespace kvs
