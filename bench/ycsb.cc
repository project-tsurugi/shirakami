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

// shirakami/test
#include "result.hh"

// shirakami/bench
#include "./include/gen_tx.hh"
#include "./include/masstree_build.hh"
#include "./include/string.hh"
#include "./include/ycsb.hh"
#include "./include/ycsb_param.h"

// shirakami/src/
#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "clock.hh"
#include "compiler.hh"
#include "cpu.hh"
#include "debug.hh"
#include "header.hh"
#include "random.hh"
#include "scheme.hh"
#include "zipf.hh"

// shirakami/include/
#include "kvs/interface.h"
#include "kvs/scheme.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

using namespace kvs;
using namespace ycsb_param;
using std::cout, std::endl, std::cerr;

DEFINE_uint64(thread, 1, "# worker threads.");
DEFINE_uint64(record, 1000, "# database records(tuples).");
DEFINE_uint64(key_length, 8, "# length of key.");
DEFINE_uint64(val_length, 8, "# length of value(payload).");
DEFINE_uint64(ops, 10, "# operations per a transaction.");
DEFINE_uint64(rratio, 100, "rate of reads in a transaction.");
DEFINE_double(skew, 0.0, "access skew of transaction.");
DEFINE_uint64(
    cpumhz, 2000,
    "# cpu MHz of execution environment. It is used measuring some time.");
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");

static void load_flags() {
  if (FLAGS_thread >= 1) {
    kNthread = FLAGS_thread;
  } else {
    cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_record > 1) {
    kCardinality = FLAGS_record;
  } else {
    cerr << "Number of database records(tuples) must be large than 0." << endl;
    exit(1);
  }
  if (FLAGS_key_length > 1 && FLAGS_key_length % 8 == 0) {
    kKeyLength = FLAGS_key_length;
  } else {
    cerr << "Length of key must be larger than 0 and be divisible by 8."
         << endl;
    exit(1);
  }
  if (FLAGS_val_length > 1) {
    kValLength = FLAGS_val_length;
  } else {
    cerr << "Length of val must be larger than 0." << endl;
    exit(1);
  }
  if (FLAGS_ops > 1) {
    kNops = FLAGS_ops;
  } else {
    cerr << "Number of operations in a transaction must be larger than 0."
         << endl;
    exit(1);
  }
  if (FLAGS_rratio >= 0 && FLAGS_rratio <= 100) {
    kRRatio = FLAGS_rratio;
  } else {
    cerr << "Rate of reads in a transaction must be in the range 0 to 100."
         << endl;
    exit(1);
  }
  if (FLAGS_skew >= 0 && FLAGS_skew < 1) {
    kZipfSkew = FLAGS_skew;
  } else {
    cerr << "Access skew of transaction must be in the range 0 to 0.999... ."
         << endl;
    exit(1);
  }
  if (FLAGS_cpumhz > 1) {
    kCPUMHz = FLAGS_cpumhz;
  } else {
    cerr << "CPU MHz of execution environment. It is used measuring some time. "
            "It must be larger than 0."
         << endl;
    exit(1);
  }
  if (FLAGS_duration >= 1) {
    kExecTime = FLAGS_duration;
  } else {
    cerr << "Duration of benchmark in seconds must be larger than 0." << endl;
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("YCSB benchmark for shirakami");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  load_flags();

  init();
  build_mtdb(kCardinality, kNthread, kValLength);
  invoke_leader();
  fin();
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
  FastZipf zipf(&rnd, kZipfSkew, kCardinality);
  Result& myres = std::ref(res[thid]);

  // this function can be used in Linux environment only.
#ifdef KVS_Linux
  setThreadAffinity(thid);
#endif

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

  Token token;
  Storage storage;
  enter(token);
  ThreadInfo* ti = static_cast<ThreadInfo*>(token);
  while (likely(!loadAcquire(quit))) {
    gen_tx_rw(ti->opr_set, kCardinality, kNops, kRRatio, rnd, zipf);
    for (auto itr = ti->opr_set.begin(); itr != ti->opr_set.end(); ++itr) {
      if (itr->get_type() == OP_TYPE::SEARCH) {
        Tuple* tuple;
        Status op_rs = search_key(token, storage, itr->get_key().data(),
                                  itr->get_key().size(), &tuple);
      } else if (itr->get_type() == OP_TYPE::UPDATE) {
        Status op_rs =
            update(token, storage, itr->get_key().data(), itr->get_key().size(),
                   itr->get_value().data(), itr->get_value().size());
      }
    }
    if (commit(token) == Status::OK) {
      ++myres.local_commit_counts_;
    } else {
      ++myres.local_abort_counts_;
      abort(token);
    }
  }
  leave(token);
}

static void invoke_leader() {
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<Result> res(kNthread);

  std::vector<char> readys(kNthread);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < kNthread; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit), std::ref(res));
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < kExecTime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto& th : thv) th.join();

  for (auto i = 0; i < kNthread; ++i) {
    res[0].addLocalAllResult(res[i]);
  }
  res[0].displayAllResult(kCPUMHz, kExecTime, kNthread);
  cout << "end experiments, start cleanup." << endl;
}
