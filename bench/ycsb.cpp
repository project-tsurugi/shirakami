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

#include <xmmintrin.h>

#include <cstring>

// shirakami/test
#include "result.h"

// shirakami/bench
#include "build_db.h"
#include "gen_tx.h"
#include "shirakami_string.h"

// shirakami/src/include
#include "atomic_wrapper.h"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/session_info.h"
#endif  // CC_SILO_VARIANT
#include "clock.h"
#include "cpu.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "tuple_local.h"

using namespace shirakami;
using namespace ycsb_param;
#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

DEFINE_uint64(thread, 1, "# worker threads.");                        // NOLINT
DEFINE_uint64(record, 100, "# database records(tuples).");            // NOLINT
DEFINE_uint64(key_length, 8, "# length of key.");                     // NOLINT
DEFINE_uint64(val_length, 4, "# length of value(payload).");          // NOLINT
DEFINE_uint64(ops, 1, "# operations per a transaction.");             // NOLINT
DEFINE_uint64(rratio, 100, "rate of reads in a transaction.");        // NOLINT
DEFINE_double(skew, 0.0, "access skew of transaction.");              // NOLINT
DEFINE_uint64(                                                        // NOLINT
    cpumhz, 2000,                                                     // NOLINT
    "# cpu MHz of execution environment. It is used measuring some "  // NOLINT
    "time.");                                                         // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");      // NOLINT

static bool isReady(const std::vector<char>& readys);  // NOLINT
static void waitForReady(const std::vector<char>& readys);
static void invoke_leader();
static void worker(size_t thid, char& ready, const bool& start,
                   const bool& quit, std::vector<Result>& res);

static void invoke_leader() {
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<Result> res(kNthread);  // NOLINT

  std::vector<char> readys(kNthread);  // NOLINT
  std::vector<std::thread> thv;
  for (std::size_t i = 0; i < kNthread; ++i) {
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit), std::ref(res));
  }
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < kExecTime; ++i) {
    sleepMs(1000);  // NOLINT
  }
  storeRelease(quit, true);
  for (auto& th : thv) th.join();

  for (std::size_t i = 0; i < kNthread; ++i) {
    res[0].addLocalAllResult(res[i]);
  }
  res[0].displayAllResult(kCPUMHz, kExecTime, kNthread);
  std::cout << "end experiments, start cleanup." << std::endl;
}

static void load_flags() {
  if (FLAGS_thread >= 1) {
    kNthread = FLAGS_thread;
  } else {
    std::cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_record > 1) {
    kCardinality = FLAGS_record;
  } else {
    std::cerr << "Number of database records(tuples) must be large than 0."
              << std::endl;
    exit(1);
  }
  constexpr std::size_t eight = 8;
  if (FLAGS_key_length > 1 && FLAGS_key_length % eight == 0) {
    kKeyLength = FLAGS_key_length;
  } else {
    std::cerr << "Length of key must be larger than 0 and be divisible by 8."
              << std::endl;
    exit(1);
  }
  if (FLAGS_val_length > 1) {
    kValLength = FLAGS_val_length;
  } else {
    std::cerr << "Length of val must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_ops >= 1) {
    kNops = FLAGS_ops;
  } else {
    std::cerr << "Number of operations in a transaction must be larger than 0."
              << std::endl;
    exit(1);
  }
  constexpr std::size_t thousand = 100;
  if (FLAGS_rratio >= 0 && FLAGS_rratio <= thousand) {
    kRRatio = FLAGS_rratio;
  } else {
    std::cerr << "Rate of reads in a transaction must be in the range 0 to 100."
              << std::endl;
    exit(1);
  }
  if (FLAGS_skew >= 0 && FLAGS_skew < 1) {
    kZipfSkew = FLAGS_skew;
  } else {
    std::cerr
        << "Access skew of transaction must be in the range 0 to 0.999... ."
        << std::endl;
    exit(1);
  }
  if (FLAGS_cpumhz > 1) {
    kCPUMHz = FLAGS_cpumhz;
  } else {
    std::cerr
        << "CPU MHz of execution environment. It is used measuring some time. "
           "It must be larger than 0."
        << std::endl;
    exit(1);
  }
  if (FLAGS_duration >= 1) {
    kExecTime = FLAGS_duration;
  } else {
    std::cerr << "Duration of benchmark in seconds must be larger than 0."
              << std::endl;
    exit(1);
  }
}

int main(int argc, char* argv[]) {  // NOLINT
  gflags::SetUsageMessage(static_cast<const std::string&>(
      "YCSB benchmark for shirakami"));  // NOLINT
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  load_flags();

  init();  // NOLINT
  build_db(kCardinality, kNthread, kValLength);
  invoke_leader();
  delete_all_records();
  fin();

  return 0;
}

bool isReady(const std::vector<char>& readys) {  // NOLINT
  for (const char& b : readys) {                 // NOLINT
    if (loadAcquire(b) == 0) return false;
  }
  return true;
}

void waitForReady(const std::vector<char>& readys) {
  while (!isReady(readys)) {
    _mm_pause();
  }
}

void worker(const size_t thid, char& ready, const bool& start, const bool& quit,
            std::vector<Result>& res) {
  // init work
  Xoroshiro128Plus rnd;
  FastZipf zipf(&rnd, kZipfSkew, kCardinality);
  std::reference_wrapper<Result> myres = std::ref(res[thid]);

  // this function can be used in Linux environment only.
#ifdef SHIRAKAMI_LINUX
  setThreadAffinity(static_cast<const int>(thid));
#endif

  Token token{};
  Storage storage{};
  std::vector<shirakami::opr_obj> opr_set;
  enter(token);

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

  while (likely(!loadAcquire(quit))) {
    gen_tx_rw(opr_set, kCardinality, kNops, kRRatio, rnd, zipf);
    for (auto&& itr : opr_set) {
      if (itr.get_type() == OP_TYPE::SEARCH) {
        Tuple* tuple{};
        search_key(token, storage, itr.get_key(), &tuple);
      } else if (itr.get_type() == OP_TYPE::UPDATE) {
        update(token, storage, itr.get_key(), itr.get_value());
      }
    }
    if (commit(token) == Status::OK) {
      ++myres.get().get_local_commit_counts();
    } else {
      ++myres.get().get_local_abort_counts();
      abort(token);
    }
  }
  leave(token);
}
