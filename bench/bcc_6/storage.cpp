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

#include <thread>

// shirakami/bench
#include "../include/gen_key.h"

// shirakami/bench/bcc_4
#include "declare_gflags.h"
#include "param.h"
#include "storage.h"

// shirakami-impl interface library
#include "random.h"

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

using namespace shirakami;

void brock_insert(Storage const st, size_t const start, size_t const end) {
    Xoroshiro128Plus rnd;
    Token token{};
    while (Status::OK != enter(token)) {
        _mm_pause(); // full session now.
    }

    auto rc{tx_begin(token)};
    if (rc != Status::OK) { LOG(FATAL) << rc; } // NOLINT

    std::size_t ctr{0};
    for (uint64_t i = start; i <= end; ++i) {
        rc = upsert(token, st, make_key(key_len, i), std::string(val_len, '0'));
        if (rc != Status::OK) { LOG(FATAL) << rc; }
        ++ctr;
        if (ctr > 10) { // NOLINT
            rc = commit(token);
            if (rc != Status::OK) { LOG(FATAL); }
            ctr = 0;
        }
    }
    rc = commit(token);
    if (rc != Status::OK) { LOG(FATAL); }
    leave(token);
}

void build_storage(Storage const st, std::size_t const rec) {
    brock_insert(st, 0, rec - 1);
}

void create_db() {
    // ddl phase
    for (std::size_t i = 0; i < FLAGS_th; ++i) {
        Storage st{};
        auto ret{create_storage(st)};
        if (ret != Status::OK) { LOG(FATAL) << "fail create_storage."; }
        get_storages().emplace_back(st);
    }

    std::vector<std::thread> ths;
    ths.reserve(FLAGS_th);
    get_storages().reserve(FLAGS_th);

    // dml phase
    for (std::size_t i = 0; i < FLAGS_th; ++i) {
        ths.emplace_back(build_storage, get_storages().at(i), rec_num);
    }

    for (auto&& th : ths) { th.join(); }
}

void init_db() {
    LOG(INFO) << "Start db initialization";
    create_db();
    LOG(INFO) << "End db initialization";
}
