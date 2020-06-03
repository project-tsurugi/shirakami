/**
 * @file error.hh
 * @brief error utilities.
 * @details This source is implemented by refering the source https://github.com/starpos/oltp-cc-bench whose the author is Takashi Hoshino.
 * And Takayuki Tanabe revised.
 */

#pragma once

#include <exception>

// class
class LibcError : public std::exception {
 private:
  std::string str_;
  static std::string generateMessage(int errnum, const std::string &msg) {
    std::string s(msg);
    const size_t BUF_SIZE = 1024;
    char buf[BUF_SIZE];
    ::snprintf(buf, 1024, " %d ", errnum);
    s += buf;
    if (::strerror_r(errnum, buf, BUF_SIZE) != nullptr) {
      s += buf;
    }
    return s;
  }

 public:
  explicit LibcError(int errnum = errno, const std::string &msg = "libc_error:")
      : str_(generateMessage(errnum, msg)) {}
};

