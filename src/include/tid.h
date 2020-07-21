/**
 * @file tid.hh
 * @brief utilities about transaction id
 */

#pragma once

#include <cstdint>

#include "epoch.h"

namespace kvs {

class TidWord {  // NOLINT
public:
  union {  // NOLINT
    uint64_t obj_;
    struct {
      bool lock_ : 1;
      bool latest_ : 1;
      bool absent_ : 1;
      uint64_t tid_ : 29;  // NOLINT
      Epoch epoch_ : 32;   // NOLINT
    };
  };

  TidWord()         // NOLINT
      : obj_(0) {}  // NOLINT : clang-tidy order to initialize other member, but
                    // it occurs compile error.
  TidWord(const uint64_t obj) { obj_ = obj; }  // NOLINT : the same as above.
  TidWord(const TidWord& right)                // NOLINT
      : obj_(right.get_obj()) {}               // NOLINT : the same as above.

  TidWord& operator=(const TidWord& right) {  // NOLINT
    obj_ = right.get_obj();                   // NOLINT : union
    return *this;
  }

  bool operator==(const TidWord& right) const {  // NOLINT : trailing
    return obj_ == right.get_obj();              // NOLINT : union
  }

  bool operator!=(const TidWord& right) const {  // NOLINT : trailing
    return !operator==(right);
  }

  bool operator<(const TidWord& right) const {  // NOLINT : trailing
    return this->obj_ < right.get_obj();        // NOLINT : union
  }

  uint64_t& get_obj() & { return obj_; }  // NOLINT

  const uint64_t& get_obj() const& { return obj_; }  // NOLINT

  bool get_lock() & { return lock_; }  // NOLINT

  [[maybe_unused]] bool get_lock() const& { return lock_; }  // NOLINT

  [[maybe_unused]] bool get_latest() & { return latest_; }  // NOLINT

  [[maybe_unused]] bool get_latest() const& { return latest_; }  // NOLINT

  bool get_absent() & { return absent_; }  // NOLINT

  [[maybe_unused]] bool get_absent() const& { return absent_; }  // NOLINT

  uint64_t get_tid() & { return tid_; }  // NOLINT

  [[maybe_unused]] uint64_t get_tid() const& { return tid_; }  // NOLINT

  Epoch get_epoch() & { return epoch_; }  // NOLINT

  [[maybe_unused]] Epoch get_epoch() const& { return epoch_; }  // NOLINT

  void inc_tid() & { this->tid_ = this->tid_ + 1; }  // NOLINT

  void reset() & { obj_ = 0; }  // NOLINT

  void set_obj(const uint64_t obj) & { this->obj_ = obj; }  // NOLINT

  void set_lock(const bool lock) & { this->lock_ = lock; }  // NOLINT

  void set_latest(const bool latest) & { this->latest_ = latest; }  // NOLINT

  void set_absent(const bool absent) & { this->absent_ = absent; }  // NOLINT

  [[maybe_unused]] void set_tid(const uint64_t tid) & {
    this->tid_ = tid;
  }  // NOLINT

  void set_epoch(const Epoch epoch) & { this->epoch_ = epoch; }  // NOLINT

  void display();

private:
};

inline std::ostream& operator<<(std::ostream& out, TidWord tid) {  // NOLINT
  tid.display();
  return out;
}

}  // namespace kvs
