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
#include "./include/gen_tx.hh"
#include "./include/result.hh"
#include "./include/string.hh"
#include "./include/ycsb.hh"
#include "./include/ycsb_param.h"

// kvs_charkey/src/
#include "include/atomic_wrapper.hh"
#include "include/cache_line_size.hh"
#include "include/clock.hh"
#include "include/compiler.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/header.hh"
#include "include/masstree_wrapper.hh"
#include "include/random.hh"
#include "include/scheme.hh"
#include "include/xact.hh"
#include "include/ycsb_param.h"
#include "include/zipf.hh"

// kvs_charkey/include/
#include "kvs/interface.h"
#include "kvs/scheme.h"

using namespace kvs;
using namespace ycsb_param;
using std::cout, std::endl;

static bool
update_parameters(std::vector<std::string> &argvs) {
  std::istringstream ss;
  auto itr = argvs.begin();
  for (itr; itr != argvs.end(); ++itr) {
    if (memcmp((*itr).data(), "--use_options", 13) == 0) {
      ++itr;
      break;
    }
  }

  for (; itr != argvs.end(); ++itr) {
    try {
      if (memcmp((*itr).data(), "-thread", strlen("-thread")) == 0) {
        ++itr;
        cout << "# threads is set to " << *itr << endl;
        kNthread = stoi(*itr);
      } else if (memcmp((*itr).data(), "-record", strlen("-record")) == 0) {
        ++itr;
        cout << "# records is set to " << *itr << endl;
        kCardinality = stoi(*itr);
      } else if (memcmp((*itr).data(), "-key_length", strlen("-key_length")) == 0) {
        ++itr;
        cout << "# length of key is set to " << *itr << endl;
        kKeyLength = stoi(*itr);
      } else if (memcmp((*itr).data(), "-val_length", strlen("-val_length")) == 0) {
        ++itr;
        cout << "# length of val is set to " << *itr << endl;
        kValLength = stoi(*itr);
      } else if (memcmp((*itr).data(), "-ops", strlen("-ops")) == 0) {
        ++itr;
        cout << "# operations is set to " << *itr << endl;
        kNops = stoi(*itr);
      } else if (memcmp((*itr).data(), "-rratio", strlen("-rratio")) == 0) {
        ++itr;
        cout << "# read ratio is set to " << *itr << endl;
        kRRatio = stoi(*itr);
      } else if (memcmp((*itr).data(), "-skew", strlen("-skew")) == 0) {
        ++itr;
        cout << "# skew is set to " << *itr << endl;
        kZipfSkew = stod(*itr);
      } else if (memcmp((*itr).data(), "-cpumhz", strlen("-cpumhz")) == 0) {
        ++itr;
        cout << "# cpuMHz is set to " << *itr << endl;
        kCPUMHz = stoi(*itr);
      } else if (memcmp((*itr).data(), "-exe_time", strlen("-exe_time")) == 0) {
        ++itr;
        cout << "# execution time is set to " << *itr << endl;
        kExecTime = stoi(*itr);
      } else {
        cout << *itr << "is an an invalid option." << endl;
        throw;
      }
    } catch (...) {
      cout << "update_parameters, catch exception." << endl;
      cout << "Usable options are -thread, -record, -key_length, -val_length -ops, -rratio, -skew, -cpumhz, -exe_time, after --use_options" << endl;
      cout << "After an option must be valid number." << endl;
      exit(1);
    } 
  }

  return true;
}

int main(int argc, char* argv[]) {
  init();
  //std::vector<std::string> argvs = testing::internal::GetArgvs();
  //update_parameters(argvs);
  std::vector<Tuple*> insertedList[kNthread];

  build_mtdb(insertedList);
  invoke_leader();
  delete_mtdb(insertedList);
  fin();
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

static void
parallel_build_mtdb(std::size_t thid, std::size_t start, std::size_t end, std::vector<Tuple*> *insertedList) {
  MasstreeWrapper<Record>::thread_init(thid);
  Token token;
  enter(token);

  for (uint64_t i = start; i <= end; ++i) {
    uint64_t keybs = __builtin_bswap64(i);
    std::unique_ptr<char[]> val = std::make_unique<char[]>(kValLength);
    make_string(val.get(), kValLength);
    Tuple *tuple = new Tuple((char *)&keybs, sizeof(uint64_t), val.get(), kValLength);
    Storage storage;
    insert(token, storage, (char *)&keybs, sizeof(uint64_t), val.get(), kValLength);
    insertedList->emplace_back(tuple);
  }
  commit(token);
  leave(token);
}

static void 
build_mtdb(std::vector<Tuple*> *insertedList)
{
  printf("ycsb::build_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber();
  printf("start parallel_build_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_build_mtdb, i, i*(kCardinality / maxthread), (i+1)*(kCardinality / maxthread) - 1, &insertedList[i]);

  for (auto &th : thv) th.join();
}

static void
parallel_delete_mtdb(std::size_t thid, std::vector<Tuple*> *insertedList)
{
  Token token;
  enter(token);
  for (auto itr = insertedList->begin(); itr != insertedList->end(); ++itr) {
    Storage storage;
    delete_record(token, storage, (*itr)->key.get(), (*itr)->len_key);
    commit(token);
    delete *itr;
  }
  leave(token);

  insertedList->clear();
}

static void
delete_mtdb(std::vector<Tuple*> *insertedList)
{
  printf("ycsb::delete_mtdb\n");
  std::vector<std::thread> thv;

  size_t maxthread = decideParallelBuildNumber();
  printf("start parallel_delete_mtdb with %zu threads.\n", maxthread);
  fflush(stdout);
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(parallel_delete_mtdb, i, &insertedList[i]);

  for (auto &th : thv) th.join();
}

static bool 
isReady(const std::vector<char>& readys)
{
  for (const char &b : readys) {
    if (!loadAcquire(b)) return false;
  }
  return true;
}

static void
waitForReady(const std::vector<char>& readys)
{
  while (!isReady(readys)) { _mm_pause(); }
}

void
worker(const size_t thid, char& ready, const bool& start, const bool& quit, std::vector<Result>& res)
{
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
    tbegin(token);
    gen_tx_rw(ti->opr_set, kCardinality, kNops, kRRatio, rnd, zipf);
    for (auto itr = ti->opr_set.begin(); itr != ti->opr_set.end(); ++itr) {
      if ((*itr).type == OP_TYPE::SEARCH) {
        Tuple *tuple;
        Status op_rs = search_key(token, storage, (*itr).key.get(), (*itr).len_key, &tuple);
      } else if ((*itr).type == OP_TYPE::UPDATE) {
        Status op_rs = update(token, storage, (*itr).key.get(), (*itr).len_key, (*itr).val.get(), (*itr).len_val);
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

static void
invoke_leader()
{
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<Result> res(kNthread);

  std::vector<char> readys(kNthread);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < kNthread; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res));
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
}
