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
   * @brief Initialization
   * @details Initialize members with 0.
   */
  void init();

  /**
   * @brief Computing check sum.
   * @details Compute the two's complement of the checksum.
   */
  void compute_two_complement_of_checksum();

  int get_checksum() { return checksum_; }

  unsigned int get_log_rec_num() { return log_rec_num_; }

  void set_checksum(int checksum) {
    this->checksum_ = checksum;
  }

private:
  int checksum_;
  unsigned int log_rec_num_{};
};

class LogRecord {
 public:
  LogRecord(){}

  LogRecord(const TidWord tid, const OP_TYPE op, const Tuple* const tuple) : tid_(tid), op_(op), tuple_(tuple) {}

  bool operator<(const LogRecord& right) {
    return this->tid_ < right.tid_;
  }

  TidWord get_tid() { return tid_; }

  OP_TYPE get_op() { return op_; }

  const Tuple* get_tuple() { return tuple_; }

  void set_tuple(Tuple* tuple) {
    this->tuple_ = tuple;
  }

  /**
   * @brief Compute checksum.
   */
  int compute_checksum();

  private:
    TidWord tid_;
    OP_TYPE op_;
    const Tuple* tuple_;
};

extern std::string kLogDirectory;

extern void single_recovery_from_log();

} // namespace kvs
