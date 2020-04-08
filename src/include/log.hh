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

extern std::string LogDirectory;

extern void single_recovery_from_log();

class LogHeader {
 public:
  int chkSum_ = 0;
  unsigned int logRecNum_ = 0;

  void init() {
    chkSum_ = 0;
    logRecNum_ = 0;
  }

  void convertChkSumIntoComplementOnTwo() {
    chkSum_ ^= 0xffffffff;
    ++chkSum_;
  }
};

class LogRecord {
 public:
  LogRecord(){}

  LogRecord(const TidWord tid, const OP_TYPE op, Tuple* const tuple) : tid_(tid), op_(op), tuple_(tuple) {}

  bool operator<(const LogRecord& right) {
    return this->tid_ < right.tid_;
  }

  void set_tuple(Tuple* tuple) {
    tuple_ = tuple;
  }

  int computeChkSum() {
    // compute checksum
    // TidWord
    int chkSum = 0;
    int* intitr = (int *)this;
    for (unsigned int i = 0; i < sizeof(TidWord) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }
    
    // OP_TYPE
    chkSum += static_cast<decltype(chkSum)>(op_);

    // key_length
    std::string_view key_view = tuple_->get_key();
    std::size_t&& key_length = key_view.size();
    intitr = (int*)&(key_length);
    for (unsigned int i = 0; i < sizeof(std::size_t) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }

    // key_body
    const char* charitr = key_view.data();
    for (std::size_t i = 0; i < key_view.size(); ++i) {
      chkSum += (*charitr);
      ++charitr;
    }

    // value_length
    std::string_view value_view = tuple_->get_value();
    std::size_t&& value_length = value_view.size();
    intitr = (int*)&(value_length);
    for (unsigned int i = 0; i < sizeof(std::size_t) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }

    // value_body
    charitr = value_view.data();
    for (std::size_t i = 0; i < value_view.size(); ++i) {
      chkSum += (*charitr);
      ++charitr;
    }

    return chkSum;
  }

  private:
    TidWord tid_;
    OP_TYPE op_;
    Tuple* tuple_;
};
} // namespace kvs
