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
    Impl() {}
    Impl(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length) {
      key_.assign(key_ptr, key_length);
      pvalue_.store(new std::string(value_ptr, value_length), std::memory_order_release);
    }
    Impl(const Impl& right);
    Impl(Impl&& right);
    Impl& operator=(const Impl& right);
    Impl& operator=(Impl&& right);
    ~Impl() {}
 
    std::string_view get_key();
    std::string_view get_value();
    void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);
    void set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value);

  private:
    std::string key_;
    std::atomic<std::string*> pvalue_;
};

Tuple::Impl::Impl(const Impl& right)
{
}

Tuple::Impl::Impl(Impl&& right)
{
}

Tuple::Impl& Tuple::Impl::operator=(const Impl& right)
{
}

Tuple::Impl& Tuple::Impl::operator=(Impl&& right)
{
}

std::string_view 
Tuple::Impl::get_key() 
{
  return std::string_view{key_.data(), key_.size()};
}

std::string_view 
Tuple::Impl::get_value() 
{
  std::string* value = pvalue_.load(std::memory_order_acquire);
  if (value != nullptr) {
    return std::string_view{value->data(), value->size()};
  } else {
    return std::string_view{};
  }
}

void 
Tuple::Impl::set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length)
{
  key_.assign(key_ptr, key_length);
  pvalue_.store(new std::string(value_ptr, value_length), std::memory_order_release);
}

void
Tuple::Impl::set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value)
{
  *old_value = pvalue_.load(std::memory_order_acquire);
  pvalue_.store(new std::string(value_ptr, value_length), std::memory_order_release);
}

Tuple::Tuple() : pimpl_(std::make_unique<Impl>()) {}

Tuple::Tuple (const char* key_ptr, const std::size_t key_length, const char* val_ptr, const std::size_t val_length) : pimpl_(std::make_unique<Impl>(key_ptr, key_length, val_ptr, val_length)) {}

Tuple::Tuple(const Tuple& right)
{
}

Tuple::Tuple(Tuple&& right)
{
}

Tuple& Tuple::operator=(const Tuple& right)
{
}

Tuple& Tuple::operator=(Tuple&& right)
{
}

Tuple::~Tuple() {};

std::string_view 
Tuple::get_key()
{
  return pimpl_.get()->get_key();
}

std::string_view 
Tuple::get_value()
{
  return pimpl_.get()->get_value();
}

void 
Tuple::set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length)
{
  pimpl_.get()->set(key_ptr, key_length, value_ptr, value_length);
}

void
Tuple::set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value)
{
  pimpl_.get()->set_value(value_ptr, value_length, old_value);
}

} // namespace kvs
