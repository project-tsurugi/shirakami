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

// shirakami/bench/bcc_8/include
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

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"


#include "glog/logging.h"

#include "gflags/gflags.h"

/**
 * general option.
 */
DEFINE_uint64(d, 1, "Duration of benchmark in seconds."); // NOLINT
DEFINE_double(skew, 0.0, "access skew of transaction.");  // NOLINT
DEFINE_uint64(tx_size, 1, "tx size.");                    // NOLINT

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

void worker(const std::size_t thid, char& ready, const bool& start,
            const bool& quit, simple_result& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, FLAGS_skew, rec_size);
    // skew 0

    setThreadAffinity(static_cast<const int>(thid));

    Token token{};
    std::vector<opr_obj> opr_set;
    opr_set.reserve(FLAGS_tx_size);
    while (Status::OK != enter(token)) { _mm_pause(); }

    std::size_t ct_commit{0};

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        gen_tx_rw(opr_set, key_size, rec_size, FLAGS_tx_size, 100, rnd, zipf);

        for (auto&& itr : opr_set) {
            Status rc{};
            if (itr.get_type() == OP_TYPE::SEARCH) {
                Tuple* tp{};
                rc = search_key(token, get_st(), itr.get_key(), tp);
                if (rc != Status::OK) {
                    LOG(FATAL) << "ec: " << rc << std::endl;
                }
            } else {
                LOG(FATAL) << "unkown operation";
            }
        }

        auto rc{commit(token)}; // NOLINT
        if (rc != Status::OK) { LOG(FATAL) << "unreachable path."; }
        ++ct_commit;
    }

    leave(token);
    res.set_ct_commit(ct_commit);
}

void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res(th_size);

    std::vector<char> readys(th_size); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < th_size; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                         std::ref(quit), std::ref(res.at(i)));
    }

    waitForReady(readys);
    LOG(INFO) << "start exp.";
    storeRelease(start, true);

    if (sleep(FLAGS_d) != 0) { LOG(FATAL) << "sleep error."; }

    storeRelease(quit, true);
    LOG(INFO) << "stop exp.";
    for (auto& th : thv) th.join();

    output_result(res);
    LOG(INFO) << "end exp, start cleanup.";
}

void init_google_logging() {
    google::InitGoogleLogging("/tmp/shirakami-bench-bcc_8");
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

    std::string log_dir = MAC2STR(PROJECT_ROOT);
    log_dir.append("/tmp/shirakami_bench_bcc_8_log");
    init(false, log_dir); // NOLINT
    init_db();
    invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) { std::cerr << e.what() << std::endl; }
