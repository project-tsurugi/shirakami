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
 * @file cliTest.cpp
 */

#include <cstdint>
#include <thread>

#include "./include/test_param.h"
#include "cpu.h"
#include "gtest/gtest.h"
#include "tuple_local.h"

// shirakami/include/
#include "kvs/interface.h"

using namespace single_thread_test;
using namespace shirakami;

std::array<std::vector<Tuple*>, Nthread> DataList{};

/**
 * @brief delete DataList object.
 * @return void
 */
static void delete_DataList() {
  for (unsigned int i = 0; i < Nthread; ++i) {
    for (auto&& itr : DataList.at(i)) {
      delete itr;  // NOLINT
    }
  }
}

static void make_string(char* string, const std::size_t len) {
  for (unsigned int i = 0; i < len - 1; ++i) {
    string[i] = rand() % 24 + 'a';  // NOLINT
  }
  // if you use printf function with %s format later,
  // the end of aray must be null chara.
  string[len - 1] = '\0';  // NOLINT
}

static void exec_insert(Token token, std::size_t thnm) {
  for (unsigned int i = 0; i < Max_insert; i++) {
    std::string key(Len_key, '0');  // NOLINT
    make_string(key.data(), Len_key);
    std::string value(Len_val, '0');  // NOLINT
    make_string(value.data(), Len_val);
    Tuple* tuple =  // NOLINT
        new Tuple(key.data(), Len_key, value.data(), Len_val);
    DataList.at(thnm).push_back(tuple);
    Storage storage(0);
    insert(token, storage, key.data(), Len_key, value.data(), Len_val);
  }
  // Commit;
  Status result = commit(token);
  ASSERT_EQ(result, Status::OK);
}

static void exec_search_key(Token token, std::size_t thnm) {
  for (auto&& itr : DataList.at(thnm)) {
    Tuple* tuple{};
    Storage storage{0};
    search_key(token, storage, itr->get_key().data(), itr->get_key().size(),
               &tuple);
  }
  Status result = commit(token);
  ASSERT_EQ(result, Status::OK);
}

static void exec_scan_key(Token token) {
  while (true) {
    std::vector<const Tuple*> result;
    Storage storage(0);
    scan_key(token, storage, static_cast<const char*>("a"), 1, false,
             static_cast<const char*>("z"), 1, false, result);
    for (auto&& itr : result) {
      delete itr;  // NOLINT
    }
    Status commit_result = commit(token);
    ASSERT_EQ(commit_result, Status::OK);
    result.clear();
    if (commit_result == Status::OK) break;
  }
}

static void exec_update(Token token, std::size_t thnm) {
  for (auto&& itr : DataList.at(thnm)) {
    Storage storage(0);
    update(token, storage, itr->get_key().data(), itr->get_key().size(),
           static_cast<const char*>("bouya-yoikoda-nenne-shina"),
           strlen("bouya-yoikoda-nenne-shina"));
  }
  Status result = commit(token);
  ASSERT_EQ(result, Status::OK);
}

static void exec_delete(const Token token, std::size_t thnm) {
  Storage storage(0);

  for (auto&& itr : DataList.at(thnm)) {
    delete_record(token, storage, itr->get_key().data(), itr->get_key().size());
  }
  Status result = commit(token);
  ASSERT_EQ(result, Status::OK);
}

static void test_insert(Token token, std::size_t thnm) {
  exec_insert(token, thnm);
}

static void test_update(Token token, std::size_t thnm) {
  exec_update(token, thnm);
}

static void test_search(Token token, std::size_t thnm) {
  exec_search_key(token, thnm);
}

static void test_scan(Token token) { exec_scan_key(token); }

static void test_delete(Token token, std::size_t thnm) {
  exec_delete(token, thnm);
}

static void test_single_operation(Token token, std::size_t thnm) {
#ifdef KVS_Linux
  setThreadAffinity(0);
#endif
  test_insert(token, thnm);
  test_search(token, thnm);
  test_update(token, thnm);
  test_delete(token, thnm);
  test_scan(token);
}

static void worker(const size_t thid) {
  Token token{};
  Status enter_result = enter(token);
  if (enter_result == Status::ERR_SESSION_LIMIT) {
    std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
              << std::endl;
    std::abort();
  }

  test_single_operation(token, thid);

  leave(token);
}

static void test() {
  std::vector<std::thread> thv;
  for (std::size_t i = 0; i < Nthread; ++i) thv.emplace_back(worker, i);

  for (auto& th : thv) th.join();

  delete_DataList();
}

namespace shirakami::testing {

class cliTest : public ::testing::Test {};

TEST_F(cliTest, single_thread_test) {  // NOLINT
  init();                              // NOLINT
  test();
  fin();
}

}  // namespace shirakami::testing
