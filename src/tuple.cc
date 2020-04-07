/**
 * @file tuple.cc
 * @brief about tuple
 */

#include <atomic>
#include <string>

#include "kvs/tuple.h"

namespace kvs{

class Tuple::Impl {
  public:
    Impl() = default;

    Impl(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length) {
      key_.assign(key_ptr, key_length);
      pvalue_.store(new std::string(value_ptr, value_length), std::memory_order_release);
    }

    ~Impl() = default;
 
    std::string_view get_key();
    std::string_view get_value();
    void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);
    void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);

  private:
    std::string key_;
    std::atomic<std::string*> pvalue_;
};

std::string_view Tuple::Impl::get_key() {
  return std::string_view{key_.data(), key_.size()};
}

std::string_view Tuple::Impl::get_value() {
  std::string* value = pvalue_.load(std::memory_order_acquire);
  if (value != nullptr) {
    return std::string_view{value->.data(), value->size()};
  } else {
    return std::string_view{};
  }
}

Tuple::Tuple() : pimpl_(std::make_unique<Impl>()) {}

Tuple::Tuple (const char* key_ptr, const std::size_t key_length, const char* val_ptr, const std::size_t val_length) : pimpl_(std::make_unique<Impl>(const char* key_ptr, const std::size_t key_length, const char* val_ptr, const std::size_t val_length)) {}

Tuple::~Tuple() {}

std::string_view Tuple::get_key()
{
  return pimpl_.get()->get_key();
}

std::string_view Tuple::get_value()
{
  return pimpl_.get()->get_value();
}

} // namespace kvs
