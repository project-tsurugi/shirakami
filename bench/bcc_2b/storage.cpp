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

// shirakami/bench/ycsb_ol_bt_nc
#include "declare_gflags.h"
#include "storage.h"

// shirakami-impl interface library
#include "random.h"

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

using namespace shirakami;

std::size_t comp_para_build_num(const std::size_t record) { // NOLINT
    // if table size is very small, it builds by single thread.
    if (record < 1000) return 1; // NOLINT

    // else
    return std::thread::hardware_concurrency();
}

void brock_insert(Storage st, size_t start, size_t end) {
    Xoroshiro128Plus rnd;
    Token token{};
    while (Status::OK != enter(token)) {
        _mm_pause(); // full session now.
    }

    auto rc{tx_begin({token})}; // NOLINT
    if (rc != Status::OK) { LOG(FATAL) << rc; } // NOLINT

    std::size_t ctr{0};
    for (uint64_t i = start; i <= end; ++i) {
        rc = upsert(token, st, make_key(FLAGS_key_len, i),
                     std::string(FLAGS_val_len, '0'));
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

void build_storage(Storage st, std::size_t rec) {
    std::size_t bl_th_num{comp_para_build_num(rec)};

    std::vector<std::thread> ths;
    for (size_t i = 0; i < bl_th_num; ++i) {
        ths.emplace_back(brock_insert, st, i * (rec / bl_th_num),
                         i != bl_th_num - 1 ? (i + 1) * (rec / bl_th_num) - 1
                                            : rec - 1);
    }

    for (auto&& th : ths) th.join();
}

void init_db_ol() {
    //std::vector<std::thread> ths;
    //ths.reserve(FLAGS_ol_thread);
    for (std::size_t i = 0; i < FLAGS_ol_thread; ++i) {
        Storage st{};
        auto ret{create_storage(st)};
        if (ret != Status::OK) { LOG(FATAL) << "fail create_storage."; }
        get_ol_storages().emplace_back(st);

        //ths.emplace_back(build_storage, st, FLAGS_ol_rec);
        build_storage(st, FLAGS_rec);
    }

    //for (auto&& th : ths) { th.join() };
}

void init_db_bt() {
    //std::vector<std::thread> ths;
    //ths.reserve(FLAGS_bt_thread);
    for (std::size_t i = 0; i < 1; ++i) {
        Storage st{};
        auto ret{create_storage(st)};
        if (ret != Status::OK) { LOG(FATAL) << "fail create_storage."; }
        get_bt_storages().emplace_back(st);

        //ths.emplace_back(build_storage, st, FLAGS_bt_rec);
        build_storage(st, FLAGS_rec);
    }

    //for (auto&& th : ths) { th.join(); }
}

void init_db() {
    LOG(INFO) << "Start db initialization";
    init_db_ol();
    init_db_bt();
    LOG(INFO) << "End db initialization";
}
