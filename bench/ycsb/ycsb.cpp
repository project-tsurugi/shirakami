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
#include "ycsb/include/gen_tx.h"

// shirakami/src/include
#include "atomic_wrapper.h"

#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"


using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64( // NOLINT
        cpumhz, 2100,
        "# cpu MHz of execution environment. It is used measuring some "
        "time.");
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");       // NOLINT
DEFINE_uint64(key_length, 8, "# length of value(payload). min is 8."); // NOLINT
DEFINE_uint64(ops, 1, "# operations per a transaction.");              // NOLINT
DEFINE_string(ops_read_type, "point", "type of read operation.");      // NOLINT
DEFINE_string(ops_write_type, "update", "type of write operation.");   // NOLINT
DEFINE_uint64(record, 10, "# database records(tuples).");              // NOLINT
DEFINE_uint64(rratio, 100, "rate of reads in a transaction.");         // NOLINT
DEFINE_uint64(scan_length, 100, "number of records in scan range.");   // NOLINT
DEFINE_double(skew, 0.0, "access skew of transaction.");               // NOLINT
DEFINE_uint64(thread, 1, "# worker threads.");                         // NOLINT
DEFINE_string(transaction_type, "short", "type of transaction.");      // NOLINT
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
    LOG(INFO) << "start waitForReady";
    waitForReady(readys);
    LOG(INFO) << "start ycsb exp.";
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
    printf("cpr_global_version:\t%zu\n", // NOLINT
           cpr::global_phase_version::get_gpv().get_version());
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

    // ops_read_type
    printf("FLAGS_ops_read_type : %s\n", // NOLINT
           FLAGS_ops_read_type.data());  // NOLINT
    if (FLAGS_ops_read_type != "point" && FLAGS_ops_read_type != "range") {
        LOG(ERROR) << log_location_prefix << "Invalid type of read operation.";
    }

    // ops_write_typea
    printf("FLAGS_ops_write_type : %s\n", // NOLINT
           FLAGS_ops_write_type.data());  // NOLINT
    if (FLAGS_ops_write_type != "update" && FLAGS_ops_write_type != "insert" &&
        FLAGS_ops_write_type != "readmodifywrite") {
        LOG(ERROR) << log_location_prefix << "Invalid type of write operation.";
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

    // transaction_type
    printf("FLAGS_transaction_type : %s\n", // NOLINT
           FLAGS_transaction_type.data());  // NOLINT
    if (FLAGS_transaction_type != "short" && FLAGS_transaction_type != "long" &&
        FLAGS_transaction_type != "read_only") {
        LOG(ERROR) << log_location_prefix << "Invalid type of transaction.";
    }

    // about thread
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
    FLAGS_stderrthreshold = 0; // to display info log
    load_flags();

    init(); // NOLINT
    LOG(INFO) << "start build_db";
    build_db(FLAGS_record, FLAGS_key_length, FLAGS_val_length, FLAGS_thread);
    LOG(INFO) << "start invoke_leader";
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
#ifdef SHIRAKAMI_LINUX
    setThreadAffinity(static_cast<const int>(thid));
#endif

    Token token{};
    std::vector<shirakami::opr_obj> opr_set;
    opr_set.reserve(FLAGS_ops);
    auto ret = enter(token);
    if (ret != Status::OK) { LOG(ERROR) << ret; }

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        // gen query contents
        gen_tx_rw(opr_set, FLAGS_key_length, FLAGS_record, FLAGS_thread, thid,
                  FLAGS_ops, FLAGS_ops_read_type, FLAGS_ops_write_type,
                  FLAGS_rratio, rnd, zipf);

        if (ret == Status::WARN_ALREADY_BEGIN) { LOG(FATAL); }

        // tx begin
        transaction_options::transaction_type tt{};
        if (FLAGS_transaction_type == "short") {
            tt = transaction_options::transaction_type::SHORT;
            ret = tx_begin({token});
        } else if (FLAGS_transaction_type == "long") {
            tt = transaction_options::transaction_type::LONG;
            ret = tx_begin({token, tt, {storage}});
            // wait start epoch
            auto* ti = static_cast<session*>(token);
            while (epoch::get_global_epoch() < ti->get_valid_epoch()) {
                _mm_pause();
            }
        } else if (FLAGS_transaction_type == "read_only") {
            tt = transaction_options::transaction_type::READ_ONLY;
            ret = tx_begin({token, tt});
        } else {
            LOG(FATAL) << log_location_prefix << "invalid transaction type";
        }
        if (ret != Status::OK) {
            LOG(FATAL) << log_location_prefix << "unexpected error. " << ret;
        }

        // execute operations
        for (auto&& itr : opr_set) {
            if (itr.get_type() == OP_TYPE::SEARCH) {
                uint64_t ctr{0};
                for (;;) {
                    std::string vb{};
                    ret = search_key(token, storage, itr.get_key(), vb);
                    if (ret == Status::OK) { break; }
                    if (ret == Status::ERR_CC) { goto ABORTED; }
                }
            } else if (itr.get_type() == OP_TYPE::UPDATE) {
                ret = update(token, storage, itr.get_key(),
                             std::string(FLAGS_val_length, '0'));
                if (ret == Status::ERR_CC) {
                    LOG(ERROR) << "unexpected error, rc: " << ret;
                }
            } else if (itr.get_type() == OP_TYPE::INSERT) {
                insert(token, storage, itr.get_key(),
                       std::string(FLAGS_val_length, '0'));
                // rarely, ret == already_exist due to design
            } else if (itr.get_type() == OP_TYPE::SCAN) {
                ScanHandle hd{};
                ret = open_scan(token, storage, itr.get_scan_l_key(),
                                scan_endpoint::INCLUSIVE, itr.get_scan_r_key(),
                                scan_endpoint::INCLUSIVE, hd,
                                FLAGS_scan_length);
                if (ret != Status::OK || ret == Status::ERR_CC) {
                    LOG(ERROR) << "unexpected error, rc: " << ret;
                }
                std::string vb{};
                do {
                    ret = read_value_from_scan(token, hd, vb);
                    if (ret == Status::ERR_CC) { goto ABORTED; }
                    if (ret != Status::OK) { LOG(ERROR) << "unexpected error"; }
                    ret = next(token, hd);
                } while (ret != Status::WARN_SCAN_LIMIT);
                ret = close_scan(token, hd);
                if (ret != Status::OK) { LOG(ERROR) << "unexpected error"; }
            }
        }
        if (commit(token) == Status::OK) { // NOLINT
            ++myres.get().get_local_commit_counts();
        } else {
        ABORTED: // NOLINT
            ++myres.get().get_local_abort_counts();
            abort(token);
        }
    }
    ret = leave(token);
    if (ret != Status::OK) { LOG(ERROR) << ret; }
}
