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

    auto rc{tx_begin({token})}; // NOLINT
    if (rc != Status::OK) { LOG(FATAL) << rc; } // NOLINT

    std::size_t ctr{0};
    for (uint64_t i = start; i <= end; ++i) {
        rc = upsert(token, st, make_key(key_size, i), std::string(val_size, '0'));
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

std::size_t comp_para_build_num(const std::size_t rec) {
    if (rec <= 10000) return 1; // NOLINT

    return std::thread::hardware_concurrency();
}

void build_storage(Storage const st, std::size_t const rec) {
    std::size_t bl_th_num{comp_para_build_num(rec)};

    std::vector<std::thread> ths;
    for (std::size_t i = 0; i < bl_th_num; ++i) {
        ths.emplace_back(brock_insert, st, i * (rec / bl_th_num),
                         i != bl_th_num - 1 ? (i + 1) * (rec / bl_th_num) - 1
                                            : rec - 1);
    }

    for (auto&& th : ths) th.join();
}

void create_db() {
    // ddl phase
    Storage st{};
    auto ret{create_storage(st)};
    if (ret != Status::OK) { LOG(FATAL) << "fail create_storage."; }
    set_st(st);

    // dml phase
    build_storage(get_st(), rec_size);
}

void init_db() {
    LOG(INFO) << "Start db initialization";
    create_db();
    LOG(INFO) << "End db initialization";
}
