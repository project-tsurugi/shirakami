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

// shirakami/test/include
#include "result.hh"

// shirakami/bench
#include "./include/masstree_build.hh"
#include "./include/string.hh"

// shirakami/src/
#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "clock.hh"
#include "compiler.hh"
#include "cpu.hh"
#include "debug.hh"
#include "header.hh"
#include "random.hh"
#include "record.hh"
#include "scheme.hh"
#include "zipf.hh"

// shirakami/include/
#include "kvs/interface.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

// to use declaration of entity of global variables.
#include "./../src/masstree_wrapper.cc"

using namespace kvs;
using std::cout, std::endl, std::cerr;

DEFINE_uint64(thread, 1, "# worker threads.");
DEFINE_uint64(record, 1000, "# database records(tuples).");
DEFINE_uint64(key_length, 8, "# length of key.");
DEFINE_uint64(val_length, 8, "# length of value(payload).");
DEFINE_uint64(
    cpumhz, 2100,
    "# cpu MHz of execution environment. It is used measuring some time.");
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");
DEFINE_string(instruction, "insert",
              "insert or put or get. The default is insert.");
DEFINE_double(skew, 0.0, "access skew of transaction.");

static void load_flags() {
  if (FLAGS_thread == 0) {
    cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_record == 0) {
    cerr << "Number of database records(tuples) must be large than 0." << endl;
    exit(1);
  }
  if (FLAGS_key_length == 0 || FLAGS_key_length % 8 != 0) {
    cerr << "Length of key must be larger than 0 and be divisible by 8."
         << endl;
    exit(1);
  }
  if (FLAGS_val_length == 0) {
    cerr << "Length of val must be larger than 0." << endl;
    exit(1);
  }
  if (FLAGS_cpumhz == 0) {
    cerr << "CPU MHz of execution environment. It is used measuring some time. "
            "It must be larger than 0."
         << endl;
    exit(1);
  }
  if (FLAGS_duration == 0) {
    cerr << "Duration of benchmark in seconds must be larger than 0." << endl;
    exit(1);
  }
  if (FLAGS_instruction == "insert" || FLAGS_instruction == "put" ||
      FLAGS_instruction == "get") {
    // ok
  } else {
    cerr << "The instruction option must be insert or put or get. The default "
            "is insert."
         << endl;
    exit(1);
  }
  if (FLAGS_skew >= 0 && FLAGS_skew < 1) {
    // ok
  } else {
    cerr << "access skew of transaction must be in the range 0 to 0.999..."
         << endl;
    exit(1);
  }
}

static bool isReady(const std::vector<char>& readys) {
  for (const char& b : readys) {
    if (!loadAcquire(b)) return false;
  }
  return true;
}

static void waitForReady(const std::vector<char>& readys) {
  while (!isReady(readys)) {
    _mm_pause();
  }
}

void worker(const size_t thid, char& ready, const bool& start, const bool& quit,
            std::vector<Result>& res) {
  // init work
  Xoroshiro128Plus rnd;
  rnd.init();
  FastZipf zipf(&rnd, FLAGS_skew, FLAGS_record);
  Result& myres = std::ref(res[thid]);

  // this function can be used in Linux environment only.
#ifdef KVS_Linux
  setThreadAffinity(thid);
#endif

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

  Token token;
  kvs::Record myrecord;
  Storage storage;
  enter(token);
  while (likely(!loadAcquire(quit))) {
    if (FLAGS_instruction == "insert") {
      std::size_t start(UINT64_MAX / FLAGS_thread * thid),
          end(UINT64_MAX / FLAGS_thread * (thid + 1));
      for (auto i = start; i < end; ++i) {
        uint64_t keybs = __builtin_bswap64(i);
        std::string value(FLAGS_val_length, '0');
        make_string(value);
        insert(token, storage, reinterpret_cast<char*>(&keybs),
               sizeof(uint64_t), value.data(), FLAGS_val_length);
        commit(token);
        ++myres.local_commit_counts_;
      }
    } else if (FLAGS_instruction == "put") {
#if 0
      // future work : If it defines that the record number is divisible by 2, it can use mask and "and computation"  instead of "surplus computation".
      // Then, it will be faster than now.
      kvs::Record* record;
      kvs::Record* newrecord = new kvs::Record();
      uint64_t keynm = zipf() % FLAGS_record;
      uint64_t keybs = __builtin_bswap64(keynm);
      if (kvs::Status::OK != MTDB.put_value((char*)&keybs, sizeof(uint64_t), newrecord, &record)) ERR;
      delete record;
      ++myres.local_commit_counts_;
    } else if (FLAGS_instruction == "get") {
      uint64_t keynm = zipf() % FLAGS_record;
      uint64_t keybs = __builtin_bswap64(keynm);
      kvs::Record* record;
      record = MTDB.get_value((char*)&keybs, sizeof(uint64_t));
      if (record == nullptr) ERR;
      ++myres.local_commit_counts_;
#endif
    }
  }
  leave(token);
}

static void invoke_leader() {
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<Result> res(FLAGS_thread);

  init();
  if (FLAGS_instruction == "put" || FLAGS_instruction == "get") {
    build_mtdb(FLAGS_record, FLAGS_thread, FLAGS_val_length);
  }

  std::vector<char> readys(FLAGS_thread);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit), std::ref(res));
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_duration; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto& th : thv) th.join();

  for (auto i = 0; i < FLAGS_thread; ++i) {
    res[0].addLocalAllResult(res[i]);
  }
  res[0].displayAllResult(FLAGS_cpumhz, FLAGS_duration, FLAGS_thread);

  fin();
}

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("YCSB benchmark for shirakami");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  load_flags();
  invoke_leader();
}
