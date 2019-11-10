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

#include "./include/ycsb_param.h"

#include "include/header.h"
#include "include/ycsb_param.h"
#include "include/xact.h"

#include <cstdint>

#include "gtest/gtest.h"

#include "kvs/debug.h"
#include "kvs/interface.h"

using namespace kvs;
using namespace ycsb_param;
using std::cout, std::endl;

std::vector<Tuple*> InsertedList[ycsb_param::kNthread];

/**
 * @brief delete InsertedList object.
 * @return void
 */
static void
delete_InsertedList()
{
  for (int i = 0; i < ycsb_param::kNthread; ++i) {
    for (auto itr = InsertedList[i].begin(); itr != InsertedList[i].end(); ++itr) {
      delete *itr;
    }
  }
}

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
  }

  void build_mtdb();
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
  for (size_t i = std::thread::hardware_concurrency(); i > 0; --i) {
    if (kCardinality % i == 0) {
      return i;
    }
    if (i == 1) ERR;
  }

  return 1;
}

void 
ycsb::build_mtdb()
{
  cout << "build masstree database." << endl;
  size_t maxthread = decideParallelBuildNumber();

  std::vector<std::thread> thv;
  for (size_t i = 0; i < kNthread; ++i) {
  }
}

}  // namespace kvs_charkey::testing
