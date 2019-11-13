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

#include "include/debug.h"

#include "./include/ycsb_param.h"

#include "include/header.hh"
#include "include/masstree_wrapper.hh"
#include "include/scheme.h"
#include "include/xact.hh"
#include "include/ycsb_param.h"

#include <cstdint>

#include "gtest/gtest.h"

#include "kvs/interface.h"
#include "kvs/scheme.h"

using namespace kvs;
using namespace ycsb_param;
using std::cout, std::endl;

std::vector<Tuple*> InsertedList[kNthread];

static void
make_string(char* string, std::size_t len)
{
    for (uint i = 0; i < len-1; i++) {
        string[i] = rand() % 24 + 'a';
    }
    // if you use printf function with %s format later,
    // the end of aray must be null chara.
    string[len-1] = '\0';
}

static void
ycsb_a()
{
}

static void
ycsb_b()
{
}

static void
ycsb_c()
{
}

namespace kvs_charkey::testing {

class ycsb : public ::testing::Test {
protected:
  ycsb() {
    init();
    build_mtdb();
    invoke_leader();
  }
  
  ~ycsb() {
    delete_mtdb();
  }

  void build_mtdb();
  void delete_mtdb();
  void invoke_leader();
}; // end of declaration of class ycsb.

TEST_F(ycsb, ycsb_a) {
  ycsb_a();
}

TEST_F(ycsb, ycsb_b) {
  ycsb_b();
}

TEST_F(ycsb, ycsb_c) {
  ycsb_c();
}

static size_t
decideParallelBuildNumber() 
{
  // if table size is very small, it builds by single thread.
  if (kCardinality < 1000) return 1;

  // else
  for (size_t i = kNthread; i > 0; --i) {
    if (kCardinality % i == 0) {
      return i;
    }
    if (i == 1) ERR;
  }

  return 1;
}

void
parallel_build_mtdb(std::size_t thid, std::size_t start, std::size_t end) {
  MasstreeWrapper<Record>::thread_init(thid);
  enter(thid);

  for (auto i = start; i <= end; ++i) {
    std::unique_ptr<char[]> key = std::make_unique<char[]>(kKeyLength);
    memcpy(key.get(), (std::to_string(i)).c_str(), kKeyLength);
    std::unique_ptr<char[]> val = std::make_unique<char[]>(kValLength);
    make_string(val.get(), kValLength);
    Tuple *tuple = new Tuple(key.get(), kKeyLength, val.get(), kValLength);
    InsertedList[thid].push_back(tuple);
    Storage storage;
    insert(thid, storage, key.get(), kKeyLength, val.get(), kValLength);
  }
  Status result = commit(thid);
  ASSERT_TRUE(result == Status::OK);

  leave(thid);
}

void 
ycsb::build_mtdb()
{
  printf("ycsb::build_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber();
  printf("start parallel_build_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_build_mtdb, i, i*(kCardinality / maxthread), (i+1)*(kCardinality / maxthread) - 1);

  for (auto &th : thv) th.join();
}

/**
 * @brief delete InsertedList object.
 * @return void
 */
static void
parallel_delete_mtdb(std::size_t thid)
{
  enter(thid);
  for (auto itr = InsertedList[thid].begin(); itr != InsertedList[thid].end(); ++itr) {
    Storage storage;
    delete_record(thid, storage, (*itr)->key.get(), (*itr)->len_key);
    commit(thid);
    delete *itr;
  }
  leave(thid);

  InsertedList[thid].clear();
}

void
ycsb::delete_mtdb()
{
  printf("ycsb::delete_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber();
  printf("start parallel_delete_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_delete_mtdb, i);

  for (auto &th : thv) th.join();
}

void
ycsb::invoke_leader()
{
}

}  // namespace kvs_charkey::testing
