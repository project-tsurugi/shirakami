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

#include <iostream>

#include <xmmintrin.h>

#include <cstring>

// shirakami/bench/bcc_4/include
#include "param.h"
#include "simple_result.h"
#include "storage.h"
#include "utility.h"

// shirakami/bench/include
#include "gen_tx.h"

// shirakami/src/include
#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"

// shirakami/src/
#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"


#include "glog/logging.h"

#include "gflags/gflags.h"

/**
 * general option.
 */
DEFINE_uint64(cr, 0, "conflict rate. [0:100] %.");        // NOLINT
DEFINE_uint64(d, 1, "Duration of benchmark in seconds."); // NOLINT

using namespace shirakami;

bool isReady(const std::vector<char>& readys) { // NOLINT
    for (const char& b : readys) {              // NOLINT
        if (loadAcquire(b) == 0) return false;
    }
    return true;
}

void waitForReady(const std::vector<char>& readys) {
    while (!isReady(readys)) { _mm_pause(); }
}

void gen_st_list(std::vector<Storage>& st_list, Xoroshiro128Plus& rnd,
                 std::size_t const thid) {
    st_list.clear();

    Storage def_st{get_bt_storages().at(thid)};
    for (std::size_t i = 0; i < tx_size; ++i) {
        Storage target_st{def_st};
        if (FLAGS_cr > (rnd.next() % 100)) { // NOLINT
            for (;;) {
                target_st = get_bt_storages().at(
                        rnd.next() % std::thread::hardware_concurrency());
                if (target_st != def_st) { break; }
            }
        }
        st_list.emplace_back(target_st);
    }
}

void worker(const std::size_t thid, const bool is_ol, char& ready,
            const bool& start, const bool& quit, simple_result& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, 0, rec_num);
    // skew 0

    setThreadAffinity(static_cast<const int>(thid));

    Token token{};
    std::vector<opr_obj> opr_set;
    opr_set.reserve(tx_size);
    std::vector<Storage> st_list;
    st_list.reserve(tx_size);
    while (Status::OK != enter(token)) { _mm_pause(); }

    std::size_t ct_abort{0};
    std::size_t ct_commit{0};

    storeRelease(ready, 1);
    while (!loadAcquire(start)) { _mm_pause(); }

    while (likely(!loadAcquire(quit))) {
        gen_tx_rw(opr_set, key_len, rec_num, tx_size, 0, rnd, zipf);

        gen_st_list(st_list, rnd, thid);
        std::vector<Storage> st_list_unique(st_list);
        std::sort(st_list_unique.begin(), st_list_unique.end());
        st_list_unique.erase(
                std::unique(st_list_unique.begin(), st_list_unique.end()),
                st_list_unique.end());

        // wp
        if (tx_begin(token, false, true, st_list_unique) != Status::OK) {
            LOG(FATAL);
        }

    RETRY: // NOLINT
        std::size_t st_ct{0};
        for (auto&& itr : opr_set) {
            Status rc{};
            if (itr.get_type() == OP_TYPE::UPDATE) {
                // update function is not implemented yet.
                rc = upsert(token, st_list.at(st_ct), itr.get_key(),
                            std::string(val_len, '0'));
                if (rc == Status::WARN_PREMATURE) {
                    goto RETRY; // NOLINT
                } else {
                    ++st_ct;
                }
                if (rc != Status::OK &&
                    rc != Status::WARN_WRITE_TO_LOCAL_WRITE) {
                    LOG(FATAL) << "ec: " << rc << std::endl;
                }
            } else {
                LOG(FATAL) << "unkown operation";
            }
        }

        if (commit(token) != Status::OK) { // NOLINT
            LOG(FATAL) << "unreachable path.";
        }
        ++ct_commit;
    }

    leave(token);
    res.set_ct_commit(ct_commit);
}

void invoke_leader() {
    LOG(INFO) << "start invoke leader.";
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    std::size_t bt_th{112}; // NOLINT
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res_bt(bt_th); // NOLINT

    std::vector<char> readys(bt_th); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < bt_th; ++i) {
        thv.emplace_back(worker, i, false, std::ref(readys[i]), std::ref(start),
                         std::ref(quit), std::ref(res_bt[i]));
    }

    LOG(INFO) << "wait leader for preparing workers.";
    waitForReady(readys);
    LOG(INFO) << "start exp.";
    storeRelease(start, true);

    if (sleep(FLAGS_d) != 0) { LOG(FATAL) << "sleep error."; }

    storeRelease(quit, true);
    LOG(INFO) << "stop exp.";
    for (auto& th : thv) th.join();

    output_result(res_bt);
    LOG(INFO) << "end exp, start cleanup.";
}

void init_google_logging() {
    google::InitGoogleLogging("/tmp/shirakami-bench-bcc_4");
    FLAGS_stderrthreshold = 0;
}

void init_gflags(int& argc, char* argv[]) { // NOLINT
    gflags::SetUsageMessage(
            static_cast<const std::string&>("YCSB benchmark for shirakami"));
    gflags::ParseCommandLineFlags(&argc, &argv, true);
}

int main(int argc, char* argv[]) try { // NOLINT
    init_google_logging();
    init_gflags(argc, argv);
    check_flags();

    init(); // NOLINT
    init_db();
    invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) { std::cerr << e.what() << std::endl; }
