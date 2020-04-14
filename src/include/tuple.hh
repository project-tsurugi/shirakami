/**
 * @file header about Tuple::Impl
 */

#pragma once

#include <atomic>
#include <string>

#include "kvs/tuple.h"

namespace kvs {

class Tuple::Impl {
  public:
    Impl() : need_delete_pvalue_(false) {
      key_.clear();
    }

    Impl(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);

    Impl(const Impl& right);
    Impl(Impl&& right);

    /**
     * @brief copy assign operator
     * @pre this is called by read_record function at xact.cc only .
     */
    Impl& operator=(const Impl& right);

    Impl& operator=(Impl&& right);

    ~Impl() {
      if (this->need_delete_pvalue_) {
        delete pvalue_.load(std::memory_order_acquire);
      } else {
      }
    }
 
    std::string_view get_key()&;
    const std::string_view get_key() const&;
    std::string_view get_value()&;
    const std::string_view get_value() const&;
    void reset()&;
    void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length)&;
    void set_key(const char* key_ptr, const std::size_t key_length)&;
    void set_value(const char* value_ptr, const std::size_t value_length)&;

    /**
     * @biref Set value
     * @details Update value and preserve old value, so this 
     * function is called by updater in write_phase.
     * @params [in] value_ptr Pointer to value
     * @params [in] value_length Size of value
     * @params [out] old_value To tell the caller the old value.
     * @return void
     */
    void set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value)&;

  private:
    std::string key_;
    std::atomic<std::string*> pvalue_;
    bool need_delete_pvalue_;
};

} // namespace kvs
