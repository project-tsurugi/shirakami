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
#include "storage.h"
#include "simple_result.h"
#include "utility.h"

// shirakami/bench/include
#include "gen_key.h"
#include "gen_tx.h"

// shirakami/src/include
#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "tuple_local.h"

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
DEFINE_uint64(val_len, 16, "# length of value(payload).");                // NOLINT

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

int main(int argc, char* argv[]) try { // NOLINT
    google::InitGoogleLogging("shirakami-bench-ycsb");
    gflags::SetUsageMessage(
            static_cast<const std::string&>("YCSB benchmark for shirakami"));
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    check_flags();

    std::string log_dir = MAC2STR(PROJECT_ROOT);
    log_dir.append("/build/bench/ycsb_ol_bt_nc_log");
    init(false, log_dir); // NOLINT
    init_db();
    //invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
}

#if 0
static bool isReady(const std::vector<char>& readys); // NOLINT
static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

static void worker(size_t thid, char& ready, const bool& start, const bool& quit, std::vector<Result>& res);

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res(FLAGS_thread); // NOLINT

    std::vector<char> readys(FLAGS_thread); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < FLAGS_thread; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res));
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

    for (std::size_t i = 0; i < FLAGS_thread; ++i) {
        res[0].addLocalAllResult(res[i]);
    }
    res[0].displayAllResult(FLAGS_cpumhz, FLAGS_duration, FLAGS_thread);
#if defined(CPR)
    printf("cpr_global_version:\t%zu\n", cpr::global_phase_version::get_gpv().get_version()); // NOLINT
#endif
    std::cout << "end experiments, start cleanup." << std::endl;
}

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

void worker(const std::size_t thid, char& ready, const bool& start,
            const bool& quit, std::vector<Result>& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, FLAGS_skew, FLAGS_record);
    std::reference_wrapper<Result> myres = std::ref(res[thid]);

    // this function can be used in Linux environment only.
#ifdef SHIRAKAMI_LINUX
    setThreadAffinity(static_cast<const int>(thid));
#endif

    Token token{};
    std::vector<shirakami::opr_obj> opr_set;
    if (thid == 0 && FLAGS_include_long_tx) {
        opr_set.reserve(FLAGS_long_tx_ops);
    } else {
        opr_set.reserve(FLAGS_ops);
    }
    enter(token);

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        if (thid == 0 && (FLAGS_include_long_tx || FLAGS_include_scan_tx)) {
            /**
             * special workloads.
             */
            if (FLAGS_include_long_tx) {
                gen_tx_rw(opr_set, FLAGS_key_len, FLAGS_record, FLAGS_long_tx_ops, FLAGS_long_tx_rratio, rnd, zipf);
                tx_begin(token, false, true);
            } else if (FLAGS_include_scan_tx) {
                gen_tx_scan(opr_set, FLAGS_key_len, FLAGS_record, FLAGS_scan_elem_num, rnd, zipf);
            } else {
                LOG(FATAL) << "fatal error";
            }
        } else {
            gen_tx_rw(opr_set, FLAGS_key_len, FLAGS_record, FLAGS_ops, FLAGS_rratio, rnd, zipf);
        }
        for (auto&& itr : opr_set) {
            if (itr.get_type() == OP_TYPE::SEARCH) {
                Tuple* tuple{};
                uint64_t ctr{0};
                for (;;) {

                    auto ret = search_key(token, storage, itr.get_key(), &tuple);
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
            } else if (itr.get_type() == OP_TYPE::SCAN) {
                tx_begin(token, true);
                std::vector<const Tuple*> scan_res;
                //                scan_key(token, storage, itr.get_scan_l_key(), scan_endpoint::INCLUSIVE, itr.get_scan_r_key(),scan_endpoint::INCLUSIVE, scan_res);
                scan_key(token, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, scan_res);
                std::cout << "list" << std::endl;
                for (auto&& it : scan_res) {
                    std::cout << it->get_key() << std::endl;
                    std::flush(std::cout);
                }
                exit(1);
#ifndef NDEBUG
                if (scan_res.size() != FLAGS_scan_elem_num) {
                    LOG(FATAL) << "scan fatal error " << scan_res.size();
                } else {
                    std::cout << "ok" << std::endl;
                }
#endif
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
    if (thid == 0 && (FLAGS_include_long_tx || FLAGS_include_scan_tx)) {
        if (FLAGS_include_long_tx) {
            printf("long_tx_commit_counts:\t%lu\n" // NOLINT
                   "long_tx_abort_counts:\t%lu\n"
                   "long_tx_throughput:\t%lu\n"
                   "long_tx_abort_rate:\t%lf\n",
                   myres.get().get_local_commit_counts(),
                   myres.get().get_local_abort_counts(),
                   myres.get().get_local_commit_counts() / FLAGS_duration,
                   static_cast<double>(myres.get().get_local_abort_counts()) /
                           static_cast<double>(myres.get().get_local_commit_counts() +
                                               myres.get().get_local_abort_counts()));
        } else if (FLAGS_include_scan_tx) {
            printf("scan_tx_commit_counts:\t%lu\n" // NOLINT
                   "scan_tx_abort_counts:\t%lu\n"
                   "scan_tx_throughput:\t%lu\n"
                   "scan_tx_abort_rate:\t%lf\n",
                   myres.get().get_local_commit_counts(),
                   myres.get().get_local_abort_counts(),
                   myres.get().get_local_commit_counts() / FLAGS_duration,
                   static_cast<double>(myres.get().get_local_abort_counts()) /
                           static_cast<double>(myres.get().get_local_commit_counts() +
                                               myres.get().get_local_abort_counts()));
        }
    }
}

#endif