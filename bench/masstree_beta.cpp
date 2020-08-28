/*
 * Copyright 2019-2019 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is the benchmark of kohler's masstree.
 */

#include <shirakami_string.h>

// shirakami/test/include
#include "result.h"

// shirakami/bench
#include "./include/build_db.h"

// shirakami/src/
#include "atomic_wrapper.h"
#include "clock.h"
#include "cpu.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "tuple_local.h"

// to use declaration of entity of global variables.
#include "index/masstree_beta/masstree_beta_wrapper.cpp"

using namespace shirakami;
#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

DEFINE_uint64(thread, 1, "# worker threads.");                // NOLINT
DEFINE_uint64(record, 1000, "# database records(tuples).");   // NOLINT
DEFINE_uint64(key_length, 8, "# length of key.");             // NOLINT
DEFINE_uint64(val_length, 8, "# length of value(payload).");  // NOLINT
DEFINE_uint64(                                                // NOLINT
    cpumhz, 2100,
    "# cpu MHz of execution environment. It is used measuring some time.");
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");  // NOLINT
DEFINE_string(instruction, "insert",                              // NOLINT
              "insert or put or get. The default is insert.");
DEFINE_double(skew, 0.0, "access skew of transaction.");  // NOLINT

static void load_flags() {
  if (FLAGS_thread == 0) {
    std::cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_record == 0) {
    std::cerr << "Number of database records(tuples) must be large than 0."
              << std::endl;
    exit(1);
  }
  if (FLAGS_key_length == 0 || FLAGS_key_length % 8 != 0) {  // NOLINT
    std::cerr << "Length of key must be larger than 0 and be divisible by 8."
              << std::endl;
    exit(1);
  }
  if (FLAGS_val_length == 0) {
    std::cerr << "Length of val must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_cpumhz == 0) {
    std::cerr
        << "CPU MHz of execution environment. It is used measuring some time. "
           "It must be larger than 0."
        << std::endl;
    exit(1);
  }
  if (FLAGS_duration == 0) {
    std::cerr << "Duration of benchmark in seconds must be larger than 0."
              << std::endl;
    exit(1);
  }
  if (FLAGS_instruction == "insert" || FLAGS_instruction == "put" ||
      FLAGS_instruction == "get") {
    // ok
  } else {
    std::cerr
        << "The instruction option must be insert or put or get. The default "
           "is insert."
        << std::endl;
    exit(1);
  }
  if (FLAGS_skew >= 0 && FLAGS_skew < 1) {
    // ok
  } else {
    std::cerr << "access skew of transaction must be in the range 0 to 0.999..."
              << std::endl;
    exit(1);
  }
}

static bool isReady(const std::vector<char>& readys) {  // NOLINT
  for (const char& b : readys) {                        // NOLINT
    if (loadAcquire(b) == 0) return false;
  }
  return true;
}

static void waitForReady(const std::vector<char>& readys) {
  while (!isReady(readys)) {
    _mm_pause();
  }
}

void worker(const std::size_t thid, char& ready, const bool& start,
            const bool& quit, std::vector<Result>& res) {
  // init work
  Xoroshiro128Plus rnd;
#if 0
  FastZipf zipf(&rnd, FLAGS_skew, FLAGS_record);
#endif
  Result& myres = std::ref(res[thid]);

  // this function can be used in Linux environment only.
#ifdef SHIRAKAMI_LINUX
  setThreadAffinity(static_cast<const int>(thid));
#endif

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

  Token token{};
  shirakami::cc_silo_variant::Record myrecord{};
  enter(token);
  while (likely(!loadAcquire(quit))) {
    if (FLAGS_instruction == "insert") {
      std::size_t begin(UINT64_MAX / FLAGS_thread * thid);
      std::size_t end(UINT64_MAX / FLAGS_thread * (thid + 1));
      for (auto i = begin; i < end; ++i) {
        uint64_t keybs = __builtin_bswap64(i);
        std::string value(FLAGS_val_length, '0');  // NOLINT
        make_string(value, rnd);
        insert(token,
               {reinterpret_cast<char*>(&keybs),  // NOLINT
                sizeof(std::uint64_t)},
               value);
        commit(token);
        ++myres.get_local_commit_counts();
        if (loadAcquire(quit)) break;
      }
    } else if (FLAGS_instruction == "put") {
#if 0
      // future work : If it defines that the record number is divisible by 2, it can use mask and "and computation"  instead of "surplus computation".
      // Then, it will be faster than now.
      shirakami::cc_silo_variant::Record* record;
      shirakami::cc_silo_variant::Record* newrecord = new shirakami::cc_silo_variant::Record();
      uint64_t keynm = zipf() % FLAGS_record;
      uint64_t keybs = __builtin_bswap64(keynm);
      if (shirakami::Status::OK != MTDB.put_value((char*)&keybs, sizeof(uint64_t), newrecord, &record)) ERR;
      delete record;
      ++myres.local_commit_counts_;
    } else if (FLAGS_instruction == "get") {
      uint64_t keynm = zipf() % FLAGS_record;
      uint64_t keybs = __builtin_bswap64(keynm);
      shirakami::cc_silo_variant::Record* record;
      record = MTDB.get_value((char*)&keybs, sizeof(uint64_t));
      if (record == nullptr) ERR;
      ++myres.local_commit_counts_;
#endif
    }
  }
  leave(token);  // NOLINT
}

static void invoke_leader() {
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<Result> res(FLAGS_thread);  // NOLINT

  std::cout << "[start] init masstree database." << std::endl;
  init();  // NOLINT
  std::cout << "[end] init masstree database." << std::endl;
  if (FLAGS_instruction == "put" || FLAGS_instruction == "get") {
    build_db(FLAGS_record, FLAGS_thread, FLAGS_val_length);
  }
  std::cout << "[report] This experiments use ";
  if (FLAGS_instruction == "insert") {
    std::cout << "insert" << std::endl;
  }

  std::vector<char> readys(FLAGS_thread);  // NOLINT
  std::vector<std::thread> thv;
  for (std::size_t i = 0; i < FLAGS_thread; ++i) {
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit), std::ref(res));
  }
  waitForReady(readys);
  storeRelease(start, true);
  std::cout << "[start] measurement." << std::endl;
  for (std::size_t i = 0; i < FLAGS_duration; ++i) {
    sleepMs(1000);  // NOLINT
  }
  std::cout << "[end] measurement." << std::endl;
  storeRelease(quit, true);
  std::cout << "[start] join worker threads." << std::endl;
  for (auto&& th : thv) th.join();
  std::cout << "[end] join worker threads." << std::endl;

  for (std::size_t i = 0; i < FLAGS_thread; ++i) {
    res[0].addLocalAllResult(res[i]);
  }
  res[0].displayAllResult(FLAGS_cpumhz, FLAGS_duration, FLAGS_thread);

  std::cout << "[start] fin masstree database." << std::endl;
  fin();
  std::cout << "[end] fin masstree database." << std::endl;
}

int main(int argc, char* argv[]) {  // NOLINT
  std::cout << "start masstree bench." << std::endl;
  gflags::SetUsageMessage(static_cast<const std::string&>(
      "YCSB benchmark for shirakami"));  // NOLINT
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  load_flags();
  invoke_leader();
  return 0;
}
