/**
 * @file header about tid
 * @brief utilities about transaction id
 */

#pragma once

#include <cstdint>

#include "epoch.hh"

namespace kvs {

class TidWord {
public:
  union {
    uint64_t obj;
    struct {
      bool lock:1;
      bool latest:1;
      bool absent:1;
      uint64_t tid:29;
      Epoch epoch:32;
    };
  };

  TidWord() { obj = 0; }
  TidWord(uint64_t obj) { obj = obj; }

  TidWord& operator=(const TidWord& right) {
    obj = right.obj;
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

  void reset() { obj = 0; }
};

} // namespace kvs
