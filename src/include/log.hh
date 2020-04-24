/**
 * @file log.hh
 * @brief Log record class.
 * @detail This source is implemented by refering the source https://github.com/thawk105/ccbench.
 */

#pragma once

#include <string.h>

#include <cstdint>

#include "fileio.hh"
#include "tid.hh"
#include "kvs/interface.h"
#include "kvs/scheme.h"

using namespace kvs;

namespace kvs {

class LogHeader {
public:
  /**
   * @brief Adds the argument to @var LogHeader::checksum_.
   */
  void add_checksum(const int add) &;

  /**
   * @brief Computing check sum.
   * @details Compute the two's complement of the checksum.
   */
  void compute_two_complement_of_checksum()&;

  /**
   * @brief Gets the value of @var LogHeader::checksum_.
   */
  [[nodiscard]] unsigned int get_checksum() const &;

  /**
   * @brief Gets the value of @var LogHeader::log_rec_num_.
   */
  [[nodiscard]] unsigned int get_log_rec_num() const &;

  /**
   * @brief Adds the one to @var LogHeader::log_rec_num_.
   */
  void inc_log_rec_num() &;

  /**
   * @brief Initialization
   * @details Initialize members with 0.
   */
  void init()&;

  /**
   * @brief Sets @var LogHeader::checksum_ to the argument.
   * @param checksum
   */
  void set_checksum(const int checksum) &;

private:
  unsigned int checksum_{};
  unsigned int log_rec_num_{};
  const unsigned int mask_full_bits_uint = 0xffffffff;
};

class LogRecord {
 public:
  LogRecord() : tid_(), op_(OP_TYPE::NONE), tuple_(nullptr) {}

  LogRecord(const TidWord tid, const OP_TYPE op, const Tuple* const tuple) : tid_(tid), op_(op), tuple_(tuple) {}

  bool operator<(const LogRecord& right) {
    return this->tid_ < right.tid_;
  }

  TidWord& get_tid() & { 
    return tid_; 
  }

  const TidWord& get_tid() const & { 
    return tid_; 
  }

  OP_TYPE& get_op() & { 
    return op_;
  }

  const OP_TYPE& get_op() const & { 
    return op_;
  }

  const Tuple* get_tuple() const & { 
    return tuple_;
  }

  void set_tuple(Tuple* tuple) {
    this->tuple_ = tuple;
  }

  /**
   * @brief Compute checksum.
   */
  int compute_checksum()&;

  private:
    TidWord tid_;
    OP_TYPE op_;
    const Tuple* tuple_;
};

extern std::string kLogDirectory;

extern void single_recovery_from_log();

} // namespace kvs
