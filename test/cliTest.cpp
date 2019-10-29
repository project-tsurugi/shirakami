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

#include "gtest/gtest.h"

#include "kernel.h"
#include "kvs/debug.h"
#include <cstdint>
#include "kvs/interface.h"
//const int Nthread = 1;
#include "test_param.h"

using namespace kvs;

std::vector<Tuple*> DataList[Nthread+1];
std::vector<Tuple*> CommonDataList;

/**
 * @brief delete DataList object.
 * @return void
 */
static void
delete_DataList()
{
  for (int i = 0; i < Nthread + 1; ++i) {
    for (auto itr = DataList[i].begin(); itr != DataList[i].end(); ++itr) {
      delete *itr;
    }
  }
}

static char *
make_string(uint len)
{
    char *string = (char *)calloc(len, sizeof(char));
    for (uint i = 0; i < len-1; i++) {
        string[i] = rand() % 24 + 'a';
    }
    string[len-1] = '\0';

    // if you use printf function with %s format later,
    // the end of aray must be null chara.
 
    return string;
}

static void
exec_insert(uint token)
{
    for (int i = 0; i < Max_insert; i++) {
        char* key = make_string(Len_key);
        char* val = make_string(Len_val);
        Tuple* tuple = new Tuple(key, Len_key, val, Len_val);
        DataList[token].push_back(tuple);
        kvs_insert(token, key, Len_key, val, Len_val);
        free(key);
        free(val);
    }

    // Commit;
    bool ok = kvs_commit(token);
    if (!ok) ERR;
}

static void
exec_search_key(uint token)
{
    //std::cout << "-------------Search-------------" << std::endl;
    for (auto itr = DataList[token].begin(); itr != DataList[token].end(); ++itr) {
        Tuple* tuple = kvs_search_key(token, (*itr)->key, (*itr)->len_key);
        if (!tuple) std::cout << "No such key" << std::endl;
        // else printf("%s:%s\n", tuple->key, tuple->val);
        delete tuple;
    }
    bool ok = kvs_commit(token);
    ASSERT_TRUE(ok);
}

static void
exec_scan_key(uint token)
{
  //std::cout << "-------------Scan-------------" << std::endl;
  while (true) {
      std::vector<Tuple*> result = kvs_scan_key(token, (char*)"a", 1, (char*)"z", 1);
      for (auto itr = result.begin(); itr != result.end(); itr++) {
          if ((*itr)->len_key == 0) std::cout << "No such key" << std::endl;
          else printf("%s:%s\n", (*itr)->key, (*itr)->val);
          delete (*itr);
      }
      bool ok = kvs_commit(token);
      result.clear();
      if (ok) break;
  }
}

static void
exec_update(const uint token)
{
    //std::cout << "-------------Update-------------" << std::endl;
    //for (auto itr = DataList[token].begin(); itr != DataList[token].end(); itr++) {
    for (auto itr = DataList[0].begin(); itr != DataList[0].end(); itr++) {
        kvs_update(token, (*itr)->key, (*itr)->len_key, (char *)"bouya-yoikoda-nenne-shina", strlen("bouya-yoikoda-nenne-shina"));
    }
    bool ok = kvs_commit(token);
    assert(ok == true);
}

static void
create_common_data_list(void)
{
  for (int i = 0; i < Max_insert; ++i) {
    char *key = make_string(Len_key);
    char *val = make_string(Len_val);
    Tuple* tuple = new Tuple(key, Len_key, val, Len_val);
    CommonDataList.push_back(tuple);
    free(key);
    free(val);
  }

  int token = kvs_enter();
  for (auto itr = CommonDataList.begin(); itr != CommonDataList.end(); itr++) {
    kvs_insert(token, (*itr)->key, (*itr)->len_key, (*itr)->val, (*itr)->len_val);
  }

  kvs_commit(token);
  kvs_leave(token);
}

static void
delete_common_data_list(void)
{
  for (auto itr = CommonDataList.begin(); itr != CommonDataList.end(); ++itr) {
    delete *itr;
  }
}

static void
exec_rmw(const uint token)
{
    while (true) {
        std::vector<Tuple*> vec_tuple;
        for (auto itr = CommonDataList.begin(); itr != CommonDataList.end(); itr++) {
            Tuple* tuple = kvs_search_key(token, (*itr)->key, (*itr)->len_key);
            if (tuple->len_key == 0) ERR;
            vec_tuple.push_back(tuple);
        }
        for (auto itr = vec_tuple.begin(); itr != vec_tuple.end(); itr++) {
            kvs_update(token, (*itr)->key, (*itr)->len_key, (char *)"bouya-yoikoda-nenne-shina", strlen("bouya-yoikoda-nenne-shina"));
        }

        bool ok = kvs_commit(token);
        vec_tuple.clear();
        if (ok == true) break;
        printf("[%d] abort\n", token);
        usleep(1000);
    }
    //assert(ok == true);
    //ERR;
}

static void
exec_delete(const uint token)
{
    //std::cout << "-------------Delete-------------" << std::endl;
    for (auto itr = DataList[token].begin(); itr != DataList[token].end(); itr++) {
        //SSS(itr->key);
        kvs_delete(token, (*itr)->key, (*itr)->len_key);
    }
    bool ok = kvs_commit(token);
    assert(ok == true);
}

static void
test_insert(const int token)
{
    printf("[%d] insert begin\n", token);
    exec_insert(token);
    printf("[%d] insert done\n", token);
    debug_print_key();
}

static void
test_update(const int token)
{
    printf("[%d] update begin\n", token);
    exec_update(token);
    printf("[%d] update done\n", token);
}

static void
test_rmw(const int token)
{
    printf("[%d] RMW begin\n", token);
    exec_rmw(token);
    printf("[%d] RMW done\n", token);
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
    test_scan(token);
    test_update(token);
    test_delete(token);
}

static void *
worker(void *a)
{
    uint token = kvs_enter();

    test_single_operation(token);
    //test_rmw(token);

    kvs_leave(token);

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
    create_common_data_list();

    pthread_t th[Nthread];
    for (int i = 0; i < Nthread; i++) {
        th[i] = thread_create();
    }

    for (int i = 0; i < Nthread; i++) {
        pthread_join(th[i], NULL);
    }

    kvs_delete_database();
    delete_common_data_list();
    delete_DataList();
}

namespace kvs_charkey::testing {

class cliTest : public ::testing::Test {
};

TEST_F(cliTest, simple_test) {
    kvs_init();
    test();
}

}  // namespace kvs_charkey::testing
