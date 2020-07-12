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

// shirakami/test
#include "result.hh"

// shirakami/bench
#include "./include/string.hh"

// shirakami-impl interface library
#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "clock.hh"
#include "compiler.hh"
#include "cpu.hh"
#include "random.hh"
#include "scheme.hh"
#include "xact.hh"
#include "zipf.hh"

// shirakami/include/
#include "kvs/interface.h"
#include "kvs/scheme.h"

using namespace kvs;
using std::cout, std::endl, std::cerr;

size_t decideParallelBuildNumber(std::size_t record, std::size_t thread) {
  // if table size is very small, it builds by single thread.
  if (record < 1000) return 1;

  // else
  for (size_t i = thread; i > 0; --i) {
    if (record % i == 0) {
      return i;
    }
    if (i == 1) {
      std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
                << std::endl;
      std::abort();
    }
  }

  return 1;
}

void parallel_build_mtdb(std::size_t thid, std::size_t start, std::size_t end,
                         std::size_t value_length) {
  Xoroshiro128Plus rnd;
  Token token;
  enter(token);

  tbegin(token);
  for (uint64_t i = start; i <= end; ++i) {
    uint64_t keybs = __builtin_bswap64(i);
    std::string val(value_length, '0');
    make_string(val, rnd);
    Storage storage;
    insert(token, storage, reinterpret_cast<char *>(&keybs), sizeof(uint64_t),
           val.data(), val.size());
  }
  commit(token);
  leave(token);
}

void build_mtdb(std::size_t record, std::size_t thread,
                std::size_t value_length) {
  printf("ycsb::build_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber(record, thread);
  printf("start parallel_build_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_build_mtdb, i, i * (record / maxthread),
                     (i + 1) * (record / maxthread) - 1, value_length);

  for (auto &th : thv) th.join();
}
