/**
 * @file
 * @brief Log record class.
 * @author Takayuki Tanabe
 * @detail This source is implemented by refering the source https://github.com/thawk105/ccbench whose the author is also Takayuki Tanabe.
 */

#pragma once

#include <string.h>

#include <cstdint>

#include "tid.hh"
#include "kvs/scheme.h"

using namespace kvs;

namespace kvs {

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
  TidWord tid_;
  OP_TYPE op_;
  Tuple tuple_;

  LogRecord() : tid_(0), tuple_() {}

  LogRecord(const TidWord tid, OP_TYPE op, const Tuple& tuple) : tid_(tid), op_(op), tuple_(tuple) {}

  LogRecord(LogRecord&& right) {
    this->tid_ = right.tid_;
    this->op_ = right.op_;
    this->tuple_ = std::move(right.tuple_);
  }

  LogRecord& operator=(LogRecord&& right) {
    this->tid_ = right.tid_;
    this->op_ = right.op_;
    this->tuple_ = std::move(right.tuple_);
  }

  bool operator<(const LogRecord& right) {
    return this->tid_ < right.tid_;
  }

  int computeChkSum() {
    // compute checksum
    int chkSum = 0;
    int* intitr = (int *)this;
    for (unsigned int i = 0; i < sizeof(TidWord) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }

    chkSum += static_cast<int32_t>(op_);

    // len_key
    for (unsigned int i = 0; i < sizeof(std::size_t) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }

    // len_val
    for (unsigned int i = 0; i < sizeof(std::size_t) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }

    // key
    char* charitr = (char*) tuple_.key.get();
    for (std::size_t i = 0; i < tuple_.len_key; ++i) {
      chkSum += (*charitr);
      ++charitr;
    }

    // val
    charitr = (char*) tuple_.val.get();
    for (std::size_t i = 0; i < tuple_.len_val; ++i) {
      chkSum += (*charitr);
      ++charitr;
    }

    return chkSum;
  }
};
} // namespace kvs
