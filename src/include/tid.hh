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

  TidWord() { obj_ = 0; }
  TidWord(const uint64_t obj) { obj_ = obj; }

  TidWord& operator=(const TidWord& right) {
    obj_ = right.get_obj();
  }

  bool operator==(const TidWord& right) const {
    return obj_ == right.get_obj();
  }

  bool operator!=(const TidWord& right) const {
    return !operator==(right);
  }

  bool operator<(const TidWord& right) const {
    return this->obj_ < right.get_obj();
  }

  uint64_t& get_obj() & {
    return obj_;
  }

  const uint64_t& get_obj() const & {
    return obj_;
  }

  bool get_lock() & { 
    return lock_;
  }

  const bool get_lock() const & { 
    return lock_;
  }

  bool get_latest() & { 
    return latest_; 
  }

  const bool get_latest() const & { 
    return latest_; 
  }

  bool get_absent() & { 
    return absent_; 
  }

  const bool get_absent() const & { 
    return absent_; 
  }

  bool get_tid() & { 
    return tid_;
  }

  const bool get_tid() const & { 
    return tid_;
  }

  bool get_epoch() & { 
    return epoch_; 
  }

  const bool get_epoch() const & { 
    return epoch_; 
  }

  void inc_tid() & {
    ++this->tid_;
  }

  void reset() & { 
    obj_ = 0;
  }

  void set_tidword(const uint64_t obj) & {
    this->obj_ = obj;
  }

  void set_lock(const bool lock) & {
    this->lock_ = lock;
  }

  void set_latest(const bool latest) & {
    this->latest_ = latest;
  }

  void set_absent(const bool absent) & {
    this->absent_ = absent;
  }

  void set_tid(const uint64_t tid) & {
    this->tid_ = tid;
  }

  void set_epoch(const Epoch epoch) & {
    this->epoch_ = epoch;
  }

private:
  union {
    uint64_t obj_;
    struct {
      bool lock_:1;
      bool latest_:1;
      bool absent_:1;
      uint64_t tid_:29;
      Epoch epoch_:32;
    };
  };
};

} // namespace kvs
