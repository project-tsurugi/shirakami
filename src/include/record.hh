/**
 * @file header about record
 * @brief utilities about record
 */

#pragma once

#include "tid.hh"
#include "kvs/scheme.h"
#include "kvs/tuple.h"

namespace kvs {
class Record {
public:
  TidWord tidw;
  Tuple tuple;

  Record () {}

  Record(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length) : tidw(), tuple(key_ptr, key_length, value_ptr, value_length) {
    // init tidw
    tidw = TidWord();
    tidw.absent = true;
    tidw.lock = true;
  }

  Record(const Record& right) = default;
  Record(Record&& right) = default;
  Record& operator=(const Record& right) = default;
  Record& operator=(Record&& right) = default;

};
} // namespace kvs
