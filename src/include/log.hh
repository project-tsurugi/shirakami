/**
 * @file
 * @brief Log record class.
 * @author Takayuki Tanabe
 * @detail This source is implemented by refering the source https://github.com/thawk105/ccbench whose the author is also Takayuki Tanabe.
 */

#pragma once

#include <string.h>

#include <cstdint>

#include "kvs/scheme.h"

using namespace kvs;

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
  uint64_t tid_;
  Tuple tuple_;

  LogRecord() : tid_(0), tuple_() {}

  LogRecord(const uint64_t tid, const Tuple& tuple) {
    this->tid_ = tid;
    this->tuple_ = tuple; // copy
  }

  int computeChkSum() {
    // compute checksum
    int chkSum = 0;
    int* intitr = (int *)this;
    for (unsigned int i = 0; i < sizeof(uint64_t) / sizeof(unsigned int); ++i) {
      chkSum += (*intitr);
      ++intitr;
    }

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
    for (unsigned char i = 0; i < tuple_.len_key; ++i) {
      chkSum += (*charitr);
      ++charitr;
    }

    // val
    charitr = (char*) tuple_.val.get();
    for (unsigned char i = 0; i < tuple_.len_val; ++i) {
      chkSum += (*charitr);
      ++charitr;
    }

    return chkSum;
  }
};
