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
#include "result.h"

// shirakami/bench
#include "./include/build_db.h"
#include "./include/shirakami_string.h"

// shirakami-impl interface library
#include "cc/silo_variant/include/interface.h"
#include "clock.h"
#include "cpu.h"
#include "random.h"
#include "tuple_local.h"

namespace shirakami {

size_t decideParallelBuildNumber(std::size_t record,  // NOLINT
                                 std::size_t thread) {
  // if table size is very small, it builds by single thread.
  if (record < 1000) return 1;  // NOLINT

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

void parallel_build_db(std::size_t start, std::size_t end,
                         std::size_t value_length) {
  Xoroshiro128Plus rnd;
  Token token{};
  enter(token);

#ifdef CC_SILO_VARIANT
  silo_variant::tx_begin(token);
#endif

  for (uint64_t i = start; i <= end; ++i) {
    uint64_t keybs = __builtin_bswap64(i);
    std::string val(value_length, '0');  // NOLINT
    make_string(val, rnd);
    Storage storage{};
    insert(token, storage, reinterpret_cast<char *>(&keybs),  // NOLINT
           sizeof(uint64_t), val.data(), val.size());
  }
  commit(token);
  leave(token);
}

void build_db(std::size_t record, std::size_t thread,
                std::size_t value_length) {
  printf("ycsb::build_mtdb\n");  // NOLINT
  std::vector<std::thread> thv;

  size_t max_thread{decideParallelBuildNumber(record, thread)};
  printf("start parallel_build_db with %zu threads.\n", max_thread);  // NOLINT
  fflush(stdout);
  for (size_t i = 0; i < max_thread; ++i) {
    thv.emplace_back(parallel_build_db, i * (record / max_thread),
                     (i + 1) * (record / max_thread) - 1, value_length);
  }

  for (auto &th : thv) th.join();
}

}