/**
 * @file tuple.h
 * @brief about tuple
 */
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace kvs {

class Tuple {
public:
  class Impl;

  Tuple();
  Tuple (const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);
  Tuple(const Tuple& right);
  Tuple(Tuple&& right);
  Tuple& operator=(const Tuple& right)&;
  Tuple& operator=(Tuple&& right)&;
  ~Tuple();

  std::string_view get_key() const&;
  std::string_view get_value() const&;
  Impl* get_pimpl() &;

private:
  std::unique_ptr<Impl> pimpl_;
};

}  // namespace kvs

