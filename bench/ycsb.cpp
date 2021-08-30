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

#include <glog/logging.h>

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
#include "glog/logging.h"
#include "concurrency_control/silo/include/tuple_local.h"

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"


using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64(                                                           // NOLINT
        cpumhz, 2100,                                                    // NOLINT
        "# cpu MHz of execution environment. It is used measuring some " // NOLINT
        "time.");                                                        // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");         // NOLINT
DEFINE_uint64(key_length, 8, "# length of value(payload). min is 8.");   // NOLINT
DEFINE_uint64(ops, 1, "# operations per a transaction.");                // NOLINT
DEFINE_uint64(record, 10, "# database records(tuples).");                // NOLINT
DEFINE_uint64(rratio, 100, "rate of reads in a transaction.");           // NOLINT
DEFINE_double(skew, 0.0, "access skew of transaction.");                 // NOLINT
DEFINE_uint64(thread, 1, "# worker threads.");                           // NOLINT
DEFINE_uint64(val_length, 4, "# length of value(payload).");             // NOLINT

/**
 * batch option.
 */
DEFINE_bool(include_long_tx, false, "If it is true, one of # worker threads executes long tx."); // NOLINT
DEFINE_uint64(long_tx_ops, 50, "# operations per long tx.");                                     // NOLINT
DEFINE_uint64(long_tx_rratio, 100, "rate of reads in long transactions.");                       // NOLINT

/**
 * scan option.
 */
DEFINE_bool(include_scan_tx, false, "If it is true, one of # worker threads executese scan tx."); // NOLINT
DEFINE_uint64(scan_elem_num, 100, "# elements in scan range.");                                   // NOLINT

static bool isReady(const std::vector<char>& readys); // NOLINT
static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

static void worker(size_t thid, char& ready, const bool& start, const bool& quit, std::vector<Result>& res);

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<Result> res(FLAGS_thread); // NOLINT

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

static void load_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_cpumhz > 1) {
        printf("FLAGS_cpumhz : %zu\n", FLAGS_cpumhz); // NOLINT
    } else {
        LOG(FATAL) << "CPU MHz of execution environment. It is used measuring some time. It must be larger than 0.";
    }
    if (FLAGS_duration >= 1) {
        printf("FLAGS_duration : %zu\n", FLAGS_duration); // NOLINT
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }
    if (FLAGS_key_length > 0) {
        printf("FLAGS_key_length : %zu\n", FLAGS_key_length); // NOLINT
    }
    if (FLAGS_ops >= 1) {
        printf("FLAGS_ops : %zu\n", FLAGS_ops); // NOLINT
    } else {
        LOG(FATAL) << "Number of operations in a transaction must be larger than 0.";
    }
    if (FLAGS_record > 1) {
        printf("FLAGS_record : %zu\n", FLAGS_record); // NOLINT
    } else {
        LOG(FATAL) << "Number of database records(tuples) must be large than 0.";
    }
    constexpr std::size_t thousand = 100;
    if (FLAGS_rratio >= 0 && FLAGS_rratio <= thousand) {
        printf("FLAGS_rratio : %zu\n", FLAGS_rratio); // NOLINT
    } else {
        LOG(FATAL) << "Rate of reads in a transaction must be in the range 0 to 100.";
    }
    if (FLAGS_skew >= 0 && FLAGS_skew < 1) {
        printf("FLAGS_skew : %f\n", FLAGS_skew); // NOLINT
    } else {
        LOG(FATAL) << "Access skew of transaction must be in the range 0 to 0.999... .";
    }
    if (FLAGS_thread >= 1) {
        printf("FLAGS_thread : %zu\n", FLAGS_thread); // NOLINT
    } else {
        LOG(FATAL) << "Number of threads must be larger than 0.";
    }
    if (FLAGS_val_length > 1) {
        printf("FLAGS_val_length : %zu\n", FLAGS_val_length); // NOLINT
    } else {
        LOG(FATAL) << "Length of val must be larger than 0.";
    }

    std::cout << "batch options" << std::endl;
    std::cout << "FLAGS_include_long_tx: " << FLAGS_include_long_tx << std::endl;
    std::cout << "FLAGS_long_tx_ops: " << FLAGS_long_tx_ops << std::endl;
    std::cout << "FLAGS_long_tx_rratio: " << FLAGS_long_tx_rratio << std::endl;

    std::cout << "scan options" << std::endl;
    std::cout << "FLAGS_include_scan_tx: " << FLAGS_include_scan_tx << std::endl;
    std::cout << "FLAGS_scan_elem_num: " << FLAGS_scan_elem_num << std::endl;

    printf("Fin load_flags()\n"); // NOLINT
}

int main(int argc, char* argv[]) try { // NOLINT
    google::InitGoogleLogging("shirakami-bench-ycsb");
    gflags::SetUsageMessage(static_cast<const std::string&>(
            "YCSB benchmark for shirakami")); // NOLINT
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    load_flags();

    /**
     * This program doesn't assume recovery.
     */
    std::string path{MAC2STR(PROJECT_ROOT)}; // NOLINT
    path += "/log/checkpoint";
    if (boost::filesystem::exists(path)) {
        boost::filesystem::remove(path);
    }

    std::string log_dir = MAC2STR(PROJECT_ROOT);
    log_dir.append("/build/bench/ycsb_log");
    init(false, log_dir); // NOLINT
    build_db(FLAGS_record, FLAGS_key_length, FLAGS_val_length, FLAGS_thread);
    invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
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
                gen_tx_rw(opr_set, FLAGS_key_length, FLAGS_record, FLAGS_long_tx_ops, FLAGS_long_tx_rratio, rnd, zipf);
                tx_begin(token, false, true);
            } else if (FLAGS_include_scan_tx) {
                gen_tx_scan(opr_set, FLAGS_key_length, FLAGS_record, FLAGS_scan_elem_num, rnd, zipf);
            } else {
                LOG(FATAL) << "fatal error";
            }
        } else {
            gen_tx_rw(opr_set, FLAGS_key_length, FLAGS_record, FLAGS_ops, FLAGS_rratio, rnd, zipf);
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
                auto ret = update(token, storage, itr.get_key(), std::string(FLAGS_val_length, '0'));
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
