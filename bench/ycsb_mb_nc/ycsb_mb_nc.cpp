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

#include <cstring>

// shirakami/test
#include "result.h"

// shirakami/bench
#include "build_db.h"
#include "gen_key.h"
#include "gen_tx.h"

// shirakami/src/include
#include "atomic_wrapper.h"

#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "gflags/gflags.h"

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"

#include "glog/logging.h"

using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64(        // NOLINT
        cpumhz, 2100, // NOLINT
        "# cpu MHz of execution environment. It is used measuring some " // NOLINT
        "time.");                                                      // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");       // NOLINT
DEFINE_uint64(key_length, 8, "# length of value(payload). min is 8."); // NOLINT
DEFINE_uint64(ops, 1, "# operations per a transaction.");              // NOLINT
DEFINE_uint64(record, 10, "# database records(tuples).");              // NOLINT
DEFINE_uint64(rratio, 100, "rate of reads in a transaction.");         // NOLINT
DEFINE_double(skew, 0.0, "access skew of transaction.");               // NOLINT
DEFINE_uint64(thread, 1, "# worker threads.");                         // NOLINT
DEFINE_uint64(val_length, 4, "# length of value(payload).");           // NOLINT

static bool isReady(const std::vector<char>& readys); // NOLINT
static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

static void worker(size_t thid, char& ready, const bool& start,
                   const bool& quit, std::vector<Result>& res);

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<Result> res(FLAGS_thread); // NOLINT

    std::vector<char> readys(FLAGS_thread); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < FLAGS_thread; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                         std::ref(quit), std::ref(res));
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
        LOG(ERROR) << log_location_prefix << "sleep error.";
    }
#endif
    storeRelease(quit, true);
    printf("stop ycsb exp.\n"); // NOLINT
    for (auto& th : thv) th.join();

    for (std::size_t i = 0; i < FLAGS_thread; ++i) {
        res[0].addLocalAllResult(res[i]);
    }
    res[0].displayAllResult(FLAGS_cpumhz, FLAGS_duration, FLAGS_thread);
#if defined(CPR)
    printf("cpr_global_version:\t%zu\n",
           cpr::global_phase_version::get_gpv().get_version()); // NOLINT
#endif
    std::cout << "end experiments, start cleanup." << std::endl;
}

static void load_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_cpumhz > 1) {
        printf("FLAGS_cpumhz : %zu\n", FLAGS_cpumhz); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "CPU MHz of execution environment. It is used measuring "
                      "some time. It must be larger than 0.";
    }
    if (FLAGS_duration >= 1) {
        printf("FLAGS_duration : %zu\n", FLAGS_duration); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Duration of benchmark in seconds must be larger than 0.";
    }
    if (FLAGS_key_length > 0) {
        printf("FLAGS_key_length : %zu\n", FLAGS_key_length); // NOLINT
    }
    if (FLAGS_ops >= 1) {
        printf("FLAGS_ops : %zu\n", FLAGS_ops); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Number of operations in a transaction must be larger "
                      "than 0.";
    }
    if (FLAGS_record > 1) {
        printf("FLAGS_record : %zu\n", FLAGS_record); // NOLINT
    } else {
        LOG(ERROR)
                << "Number of database records(tuples) must be large than 0.";
    }
    constexpr std::size_t thousand = 100;
    if (FLAGS_rratio >= 0 && FLAGS_rratio <= thousand) {
        printf("FLAGS_rratio : %zu\n", FLAGS_rratio); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Rate of reads in a transaction must be in the range 0 "
                      "to 100.";
    }
    if (FLAGS_skew >= 0 && FLAGS_skew < 1) {
        printf("FLAGS_skew : %f\n", FLAGS_skew); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Access skew of transaction must be in the range 0 to "
                      "0.999... .";
    }
    if (FLAGS_thread >= 1) {
        printf("FLAGS_thread : %zu\n", FLAGS_thread); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Number of threads must be larger than 0.";
    }
    if (FLAGS_val_length > 1) {
        printf("FLAGS_val_length : %zu\n", FLAGS_val_length); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Length of val must be larger than 0.";
    }

    printf("Fin load_flags()\n"); // NOLINT
}

int main(int argc, char* argv[]) try { // NOLINT
    google::InitGoogleLogging("shirakami-bench-ycsb");
    gflags::SetUsageMessage(static_cast<const std::string&>(
            "YCSB benchmark for shirakami")); // NOLINT
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    load_flags();

    init(); // NOLINT

    /**
     * about separating
     */
    set_use_separate_storage(true);

    build_db(FLAGS_record, FLAGS_key_length, FLAGS_val_length, FLAGS_thread);
    invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) { std::cerr << e.what() << std::endl; }

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
            const bool& quit, std::vector<Result>& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, FLAGS_skew, FLAGS_record);
    std::reference_wrapper<Result> myres = std::ref(res[thid]);

    // this function can be used in Linux environment only.
    setThreadAffinity(static_cast<const int>(thid));

    Token token{};
    std::vector<shirakami::opr_obj> opr_set;
    enter(token);

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        opr_set.reserve(FLAGS_ops);
        gen_tx_rw(opr_set, FLAGS_key_length, FLAGS_record, FLAGS_ops,
                  FLAGS_rratio, rnd, zipf);
        tx_begin({token, // NOLINT
                  transaction_options::transaction_type::LONG});
        for (auto&& itr : opr_set) {
            if (itr.get_type() == OP_TYPE::SEARCH) {
                uint64_t ctr{0};
                for (;;) {
                    std::string vb{};
                    auto ret = search_key(token, get_separate_storage()[thid],
                                          itr.get_key(), vb);
                    if (ret == Status::OK) break;
                }
            } else if (itr.get_type() == OP_TYPE::UPDATE) {
                auto ret = update(token, get_separate_storage()[thid],
                                  itr.get_key(),
                                  std::string(FLAGS_val_length, '0'));
            } else {
                LOG(ERROR) << log_location_prefix << "error.";
            }
        }
        if (commit(token) == Status::OK) { // NOLINT
            ++myres.get().get_local_commit_counts();
        } else {
            ++myres.get().get_local_abort_counts();
            abort(token);
        }
    }
    leave(token);
}
