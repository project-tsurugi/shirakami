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

#include "./include/test_param.h"

// kvs_charkey/src/
#include "include/kernel.h"
#include "include/xact.hh"

#include <cstdint>

#include "gtest/gtest.h"

// kvs_charkey/include/
#include "kvs/debug.h"
#include "kvs/interface.h"

using namespace single_thread_test;
using namespace kvs;

std::vector<Tuple*> DataList[Nthread];

/**
 * @brief delete DataList object.
 * @return void
 */
static void
delete_DataList()
{
  for (int i = 0; i < Nthread; ++i) {
    for (auto itr = DataList[i].begin(); itr != DataList[i].end(); ++itr) {
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
exec_insert(Token token)
{
  std::cout << "-------------Insert-------------" << std::endl;
  for (int i = 0; i < Max_insert; i++) {
    std::unique_ptr<char[]> key = std::make_unique<char[]>(Len_key);
    make_string(key.get(), Len_key);
    std::unique_ptr<char[]> val = std::make_unique<char[]>(Len_val);
    make_string(val.get(), Len_val);
    Tuple* tuple = new Tuple(key.get(), Len_key, val.get(), Len_val);
    DataList[token].push_back(tuple);
    Storage storage;
    insert(token, storage, key.get(), Len_key, val.get(), Len_val);
  }
  // Commit;
  Status result = commit(token);
  ASSERT_TRUE(result == Status::OK);
}

static void
exec_search_key(Token token)
{
    std::cout << "-------------Search-------------" << std::endl;
    for (auto itr = DataList[token].begin(); itr != DataList[token].end(); ++itr) {
        Tuple* tuple;
        Storage storage;
        Status search_result = search_key(token, storage, (*itr)->key.get(), (*itr)->len_key, &tuple);
        if (search_result == Status::ERR_NOT_FOUND) std::cout << "No such key" << std::endl;
        // else if (search_result == Status::OK) printf("%s:%s\n", tuple->key, tuple->val);
    }
    Status result = commit(token);
    ASSERT_TRUE(result == Status::OK);
}

static void
exec_scan_key(Token token)
{
  //std::cout << "-------------Scan-------------" << std::endl;
  while (true) {
    std::vector<Tuple*> result;
    Storage storage;
    scan_key(token, storage,
        (char*)"a", 1, false,
        (char*)"z", 1, false,
        result);
    for (auto itr = result.begin(); itr != result.end(); itr++) {
      if ((*itr)->len_key == 0) std::cout << "No such key" << std::endl;
        //else printf("%s:%s\n", (*itr)->key, (*itr)->val);
        delete (*itr);
    }
    Status commit_result = commit(token);
    ASSERT_TRUE(commit_result == Status::OK);
    result.clear();
    if (commit_result == Status::OK) break;
  }
}

static void
exec_update(Token token)
{
  std::cout << "-------------Update-------------" << std::endl;
  for (auto itr = DataList[token].begin(); itr != DataList[token].end(); itr++) {
    Storage storage;
    Status update_result = update(token, storage, (*itr)->key.get(), (*itr)->len_key, (char *)"bouya-yoikoda-nenne-shina", strlen("bouya-yoikoda-nenne-shina"));
  }
  Status result = commit(token);
  ASSERT_TRUE(result == Status::OK);
}

static void
exec_delete(const Token token)
{
  Storage storage;

  std::cout << "-------------Delete-------------" << std::endl;
  for (auto itr = DataList[token].begin(); itr != DataList[token].end(); itr++) {
    //SSS(itr->key);
    delete_record(token, storage, (*itr)->key.get(), (*itr)->len_key);
  }
  Status result = commit(token);
  ASSERT_TRUE(result == Status::OK);
}

static void
test_insert(const int token)
{
    printf("[%d] insert begin\n", token);
    exec_insert(token);
    printf("[%d] insert done\n", token);
    print_MTDB();
}

static void
test_update(const int token)
{
    printf("[%d] update begin\n", token);
    exec_update(token);
    printf("[%d] update done\n", token);
}

static void
test_search(const int token)
{
    printf("[%d] search begin\n", token);
    exec_search_key(token);
    printf("[%d] search done\n", token);
}

static void
test_scan(const int token)
{
    printf("[%d] scan begin\n", token);
    exec_scan_key(token);
    printf("[%d] scan done\n", token);
}

static void
test_delete(const int token)
{
    printf("[%d] delete begin\n", token);
    exec_delete(token);
    printf("[%d] delete done\n", token);
}

static void
test_single_operation(const int token)
{
  test_insert(token);
  test_search(token);
  test_update(token);
  test_delete(token);
  //test_scan(token);
}

static void *
worker(void *a)
{
  Token token = TokenForExp::GetToken();
  Status enter_result = enter(token);
  if (enter_result == Status::WARN_ALREADY_IN_A_SESSION) ERR;

  test_single_operation(token);

  leave(token);
  return nullptr;
}

static pthread_t
thread_create(void)
{
    pthread_t t;
    if (pthread_create(&t, NULL, worker, (void *)NULL)) ERR;

    return t;
}

static void
test(void)
{
  pthread_t th[Nthread];
  for (int i = 0; i < Nthread; i++) {
    th[i] = thread_create();
  }

  for (int i = 0; i < Nthread; i++) {
    pthread_join(th[i], NULL);
  }

  delete_DataList();
}

namespace kvs_charkey::testing {

class cliTest : public ::testing::Test {
};

TEST_F(cliTest, single_thread_test) {
  init();
  test();
}

}  // namespace kvs_charkey::testing
