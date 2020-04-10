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
  Tuple();
  Tuple (const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);
  Tuple(const Tuple& right);
  Tuple(Tuple&& right);
  Tuple& operator=(const Tuple& right)&;
  Tuple& operator=(Tuple&& right)&;
  ~Tuple();

  std::string_view get_key()&;
  std::string_view get_key() const&;
  std::string_view get_value()&;
  std::string_view get_value() const&;

  void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length)&;

  /**
   * @brief set key of data in local
   * @details The memory area of old local data is overwritten..
   */
  void set_key(const char* key_ptr, const std::size_t key_length)&;

  /**
   * @brief set value of data in local
   * @details The memory area of old local data is released immediately.
   */
  void set_value(const char* value_ptr, const std::size_t value_length)&;

  /**
   * @brief set value of data in global
   * @details The memory area of old local data is managed by GabeColle.
   * @params [out] old_value Tell the information to pass to GabeColle.
   */
  void set_value(const char* value_ptr, const std::size_t value_length, std::string** const old_value)&;

private:
  class Impl;
  std::unique_ptr<Impl> pimpl_;
};

}  // namespace kvs

