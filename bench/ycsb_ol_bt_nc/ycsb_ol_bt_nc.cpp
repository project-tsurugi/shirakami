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

// shirakami/bench/ycsb_ol_bt_nc/include
#include "simple_result.h"
#include "storage.h"
#include "utility.h"

// shirakami/bench/include
#include "gen_key.h"
#include "gen_tx.h"

// shirakami/src/include
#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "concurrency_control/include/tuple_local.h"

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"

#include "glog/logging.h"

#include "gflags/gflags.h"

using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64(                                                           // NOLINT
        cpumhz, 2100,                                                    // NOLINT
        "# cpu MHz of execution environment. It is used measuring some " // NOLINT
        "time.");                                                        // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");         // NOLINT
DEFINE_uint64(key_len, 8, "# length of value(payload). min is 8.");      // NOLINT
DEFINE_uint64(val_len, 16, "# length of value(payload).");               // NOLINT

/**
 * about online tx
 */
DEFINE_uint64(ol_ops, 1, "# operations per a online tx.");               // NOLINT
DEFINE_uint64(ol_rratio, 100, "rate of reads in a online tx.");          // NOLINT
DEFINE_uint64(ol_rec, 10, "# database records for each online worker."); // NOLINT
DEFINE_double(ol_skew, 0.0, "access skew of online tx.");                // NOLINT
DEFINE_uint64(ol_thread, 2, "# online worker threads.");                 // NOLINT

/**
 * about batch tx
 */
DEFINE_uint64(bt_ops, 1, "# operations per a batch tx.");               // NOLINT
DEFINE_uint64(bt_rratio, 100, "rate of reads in a batch tx.");          // NOLINT
DEFINE_uint64(bt_rec, 10, "# database records for each batch worker."); // NOLINT
DEFINE_double(bt_skew, 0.0, "access skew of batch tx.");                // NOLINT
DEFINE_uint64(bt_thread, 2, "# batch worker threads.");                 // NOLINT

bool isReady(const std::vector<char>& readys) { // NOLINT
    for (const char& b : readys) {              // NOLINT
        if (loadAcquire(b) == 0) return false;
    }
    return true;
}

void waitForReady(const std::vector<char>& readys) {
    while (!isReady(readys)) {
        _mm_pause();
    }
}

void worker(const std::size_t thid, const bool is_ol, char& ready, const bool& start,
            const bool& quit, simple_result& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, is_ol ? FLAGS_ol_skew : FLAGS_bt_skew, is_ol ? FLAGS_ol_rec : FLAGS_bt_rec);

    setThreadAffinity(static_cast<const int>(is_ol ? thid : thid + FLAGS_ol_thread));

    Token token{};
    Storage storage{is_ol ? get_ol_storages()[thid] : get_bt_storages()[thid]};
    std::vector<shirakami::opr_obj> opr_set;
    opr_set.reserve(is_ol ? FLAGS_ol_ops : FLAGS_bt_ops);
    while (Status::OK != enter(token)) {
        _mm_pause();
    }

    std::size_t ct_abort{0};
    std::size_t ct_commit{0};

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        gen_tx_rw(opr_set, FLAGS_key_len,
                  is_ol ? FLAGS_ol_rec : FLAGS_bt_rec,
                  is_ol ? FLAGS_ol_ops : FLAGS_bt_ops,
                  is_ol ? FLAGS_ol_rratio : FLAGS_bt_rratio,
                  rnd, zipf);

        if (!is_ol) {
            tx_begin(token, TX_TYPE::LONG);
        }

        for (auto&& itr : opr_set) {
            if (itr.get_type() == OP_TYPE::SEARCH) {
                uint64_t ctr{0};
                for (;;) {
                    std::string vb{};
                    auto ret = search_key(token, storage, itr.get_key(), vb);
                    if (ret == Status::OK || ret == Status::WARN_READ_FROM_OWN_OPERATION) break;
#ifndef NDEBUG
                    assert(ret == Status::WARN_CONCURRENT_UPDATE); // NOLINT
#endif
                }
            } else if (itr.get_type() == OP_TYPE::UPDATE) {
                auto ret = update(token, storage, itr.get_key(), std::string(FLAGS_val_len, '0'));
#ifndef NDEBUG
                assert(ret == Status::OK || ret == Status::WARN_WRITE_TO_LOCAL_WRITE); // NOLINT
#endif
            }
        }
        if (commit(token) == Status::OK) { // NOLINT
            ++ct_commit;
        } else {
            ++ct_abort;
            abort(token);
        }
    }
    leave(token);

    res.set_ct_abort(ct_abort);
    res.set_ct_commit(ct_commit);
}

void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res_ol(FLAGS_ol_thread); // NOLINT
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res_bt(FLAGS_bt_thread); // NOLINT

    std::vector<char> readys(FLAGS_ol_thread + FLAGS_bt_thread); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < FLAGS_ol_thread; ++i) {
        thv.emplace_back(worker, i, true, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res_ol[i]));
    }
    for (std::size_t i = 0; i < FLAGS_bt_thread; ++i) {
        thv.emplace_back(worker, i, false, std::ref(readys[i + FLAGS_ol_thread]), std::ref(start), std::ref(quit), std::ref(res_bt[i]));
    }
    waitForReady(readys);
    printf("start ycsb exp.\n"); // NOLINT
    storeRelease(start, true);
#if 0
    for (size_t i = 0; i < FLAGS_duration; ++i) {
        sleepMs(1000);  // NOLINT
    }
#else
    if (sleep(FLAGS_duration) != 0) {
        LOG(FATAL) << "sleep error.";
    }
#endif
    storeRelease(quit, true);
    printf("stop ycsb exp.\n"); // NOLINT
    for (auto& th : thv) th.join();

    output_result(res_ol, res_bt);
    std::cout << "end experiments, start cleanup." << std::endl;
}

int main(int argc, char* argv[]) try { // NOLINT
    google::InitGoogleLogging("shirakami-bench-ycsb");
    gflags::SetUsageMessage(
            static_cast<const std::string&>("YCSB benchmark for shirakami"));
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    check_flags();

    init(); // NOLINT
    init_db();
    invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
}
