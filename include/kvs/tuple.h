/**
 * @file tuple.h
 * @brief about tuple
 */
#pragma once

namespace kvs {

class Tuple {
public:
  Tuple();

  Tuple (const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);

  ~Tuple();

  std::string_view get_key();
  std::string_view get_value();
  void set(const char* key_ptr, const std::size_t key_length, const char* value_ptr, const std::size_t value_length);
  void set_value(const char* value_ptr, const std::size_t value_length);

private:
  class Impl;
  std::unique_ptr<Impl> pimpl_{};
};

}  // namespace kvs

