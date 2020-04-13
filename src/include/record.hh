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
  Record () : tidw_(), tuple_() {}

  Record(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length) : tidw_(), tuple_(key_ptr, key_length, value_ptr, value_length) {
    // init tidw
    tidw_.set_absent(true);
    tidw_.set_lock(true);
  }

  Record(const Record& right) = default;
  Record(Record&& right) {
    tidw_ = right.tidw_;
    tuple_ = std::move(right.tuple_);
  }

  Record& operator=(const Record& right) = default;
  Record& operator=(Record&& right) {
    tidw_ = right.tidw_;
    tuple_ = std::move(right.tuple_);

    return *this;
  }


  TidWord& get_tidw() {
    return tidw_;
  }

  const TidWord& get_tidw() const {
    return tidw_;
  }

  Tuple& get_tuple(){
    return tuple_;
  }

  const Tuple& get_tuple() const {
    return tuple_;
  }

  void set_tidw(TidWord tidw) & {
    tidw_.set_obj(tidw.get_obj());
  }

private:
  TidWord tidw_;
  Tuple tuple_;
};

} // namespace kvs
