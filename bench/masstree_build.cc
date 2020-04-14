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

// kvs_charkey/test
#include "result.hh"

// kvs_charkey/bench
#include "./include/string.hh"

// kvs_charkey-impl interface library
#include "atomic_wrapper.hh"
#include "cache_line_size.hh"
#include "clock.hh"
#include "compiler.hh"
#include "cpu.hh"
#include "debug.hh"
#include "header.hh"
#include "random.hh"
#include "scheme.hh"
#include "xact.hh"
#include "zipf.hh"

// kvs_charkey/include/
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
    if (i == 1) ERR;
  }

  return 1;
}

void parallel_build_mtdb(std::size_t thid, std::size_t start, std::size_t end,
                         std::size_t len_val,
                         std::vector<Tuple *> *insertedList) {
  Token token;
  enter(token);

  tbegin(token);
  for (uint64_t i = start; i <= end; ++i) {
    uint64_t keybs = __builtin_bswap64(i);
    std::unique_ptr<char[]> val = std::make_unique<char[]>(len_val);
    make_string(val.get(), len_val);
    Tuple *tuple =
        new Tuple((char *)&keybs, sizeof(uint64_t), val.get(), len_val);
    Storage storage;
    insert(token, storage, (char *)&keybs, sizeof(uint64_t), val.get(),
           len_val);
    insertedList->emplace_back(tuple);
  }
  commit(token);
  leave(token);
}

void build_mtdb(std::size_t record, std::size_t thread, std::size_t len_val,
                std::vector<Tuple *> *insertedList) {
  printf("ycsb::build_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber(record, thread);
  printf("start parallel_build_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_build_mtdb, i, i * (record / maxthread),
                     (i + 1) * (record / maxthread) - 1, len_val,
                     &insertedList[i]);

  for (auto &th : thv) th.join();
}

void parallel_delete_mtdb(std::size_t thid,
                          std::vector<Tuple *> *insertedList) {
  Token token;
  enter(token);
  for (auto itr = insertedList->begin(); itr != insertedList->end(); ++itr) {
    tbegin(token);
    Storage storage;
    delete_record(token, storage, (*itr)->get_key().data(),
                  (*itr)->get_key().size());
    commit(token);
    delete *itr;
  }
  leave(token);

  insertedList->clear();
}

void delete_mtdb(std::size_t record, std::size_t thread,
                 std::vector<Tuple *> *insertedList) {
  printf("ycsb::delete_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber(record, thread);
  printf("start parallel_delete_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_delete_mtdb, i, &insertedList[i]);

  for (auto &th : thv) th.join();
}
