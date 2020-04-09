/**
 * @file tuple.cc
 * @brief about tuple
 */

#include <atomic>
#include <cassert>
#include <string>

#include "kvs/tuple.h"

namespace kvs{

class Tuple::Impl {
  public:
    Impl() : need_delete_pvalue_(false) {
      pvalue_.store(nullptr, std::memory_order_release);
    }

    Impl(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length) : need_delete_pvalue_(true) {
      key_.assign(key_ptr, key_length);
      pvalue_.store(new std::string(value_ptr, value_length), std::memory_order_release);
    }

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
      }
    }
 
    std::string_view get_key() const;
    std::string_view get_value() const;
    void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);
    void set_key(const char* key_ptr, const std::size_t key_length);
    void set_value(const char* value_ptr, const std::size_t value_length);

    /**
     * @biref Set value
     * @details Update value and preserve old value, so this 
     * function is called by updater in write_phase.
     * @params [in] value_ptr Pointer to value
     * @params [in] value_length Size of value
     * @params [out] old_value To tell the caller the old value.
     * @return void
     */
    void set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value);

  private:
    std::string key_;
    std::atomic<std::string*> pvalue_;
    bool need_delete_pvalue_;
};

Tuple::Impl::Impl(const Impl& right)
{
  this->key_ = right.key_;
  if (right.need_delete_pvalue_) {
    this->need_delete_pvalue_ = true;
    this->pvalue_.store(new std::string(*right.pvalue_.load(std::memory_order_acquire)), std::memory_order_release);
  } else {
    this->need_delete_pvalue_ = false;
    this->pvalue_.store(nullptr, std::memory_order_release);
  }
}

Tuple::Impl::Impl(Impl&& right)
{
  this->key_ = std::move(right.key_);
  if (right.need_delete_pvalue_) {
    this->need_delete_pvalue_ = true;
    this->pvalue_.store(right.pvalue_.load(std::memory_order_acquire), std::memory_order_release);
    right.need_delete_pvalue_ = false;
  } else {
    this->need_delete_pvalue_ = false;
    this->pvalue_.store(nullptr, std::memory_order_release);
  }
}

Tuple::Impl& Tuple::Impl::operator=(const Impl& right)
{
  // process about this
  if (this->need_delete_pvalue_) {
    delete this->pvalue_.load(std::memory_order_acquire);
  }
  // process about copy assign
  this->key_ = right.key_;
  if (right.need_delete_pvalue_) {
    this->need_delete_pvalue_ = true;
    this->pvalue_.store(new std::string(*right.pvalue_.load(std::memory_order_acquire)), std::memory_order_release);
  } else {
    this->need_delete_pvalue_ = false;
    this->pvalue_.store(nullptr, std::memory_order_release);
  }
}

Tuple::Impl& Tuple::Impl::operator=(Impl&& right)
{
  // process about this
  if (this->need_delete_pvalue_) {
    delete this->pvalue_.load(std::memory_order_acquire);
  }
  // process about move assign
  this->key_ = std::move(right.key_);
  if (right.need_delete_pvalue_) {
    this->need_delete_pvalue_ = true;
    this->pvalue_.store(right.pvalue_.load(std::memory_order_acquire), std::memory_order_release);
    right.need_delete_pvalue_ = false;
  } else {
    this->need_delete_pvalue_ = false;
    this->pvalue_.store(nullptr, std::memory_order_release);
  }
}

std::string_view 
Tuple::Impl::get_key()  const
{
  return std::string_view{key_.data(), key_.size()};
}

std::string_view 
Tuple::Impl::get_value()  const
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
  this->need_delete_pvalue_ = true;
}

void 
Tuple::Impl::set_key(const char* key_ptr, const std::size_t key_length){
  key_.assign(key_ptr, key_length);
}

void
Tuple::Impl::set_value(const char* value_ptr, const std::size_t value_length)
{
  pvalue_.load(std::memory_order_acquire)->assign(value_ptr, value_length);
  this->need_delete_pvalue_ = true;
}

void
Tuple::Impl::set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value)
{
  *old_value = pvalue_.load(std::memory_order_acquire);
  pvalue_.store(new std::string(value_ptr, value_length), std::memory_order_release);
  this->need_delete_pvalue_ = true;
}

Tuple::Tuple() : pimpl_(std::make_unique<Impl>()) {}

Tuple::Tuple (const char* key_ptr, const std::size_t key_length, const char* val_ptr, const std::size_t val_length) : pimpl_(std::make_unique<Impl>(key_ptr, key_length, val_ptr, val_length)) {}

Tuple::Tuple(const Tuple& right)
{
  pimpl_.reset();
  pimpl_ = std::make_unique<Impl>(*right.pimpl_.get());
}

Tuple::Tuple(Tuple&& right)
{
  pimpl_ = std::move(right.pimpl_);
}

Tuple& Tuple::operator=(const Tuple& right)
{
  this->pimpl_.reset();
  this->pimpl_ = std::make_unique<Impl>(*right.pimpl_.get());
}

Tuple& Tuple::operator=(Tuple&& right)
{
  this->pimpl_ = std::move(right.pimpl_);
}

Tuple::~Tuple() {};

std::string_view 
Tuple::get_key() const
{
  return pimpl_.get()->get_key();
}

std::string_view 
Tuple::get_value() const
{
  return pimpl_.get()->get_value();
}

void 
Tuple::set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length)
{
  pimpl_.get()->set(key_ptr, key_length, value_ptr, value_length);
}

void
Tuple::set_value(const char* value_ptr, const std::size_t value_length)
{
  pimpl_.get()->set_value(value_ptr, value_length);
}

void
Tuple::set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value)
{
  pimpl_.get()->set_value(value_ptr, value_length, old_value);
}

} // namespace kvs
