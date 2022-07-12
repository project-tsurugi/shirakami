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

// shirakami/bench/bcc_3b_st/include
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

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"

#include "glog/logging.h"

#include "gflags/gflags.h"

using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64(        // NOLINT
        cpumhz, 2100, // NOLINT
        "# cpu MHz of execution environment. It is used measuring some " // NOLINT
        "time.");                                                   // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");    // NOLINT
DEFINE_uint64(key_len, 8, "# length of value(payload). min is 8."); // NOLINT
DEFINE_uint64(val_len, 8, "# length of value(payload).");           // NOLINT
DEFINE_uint64(rec, 10,                                              // NOLINT
              "# database records for each worker.");

/**
 * about online tx
 */
DEFINE_uint64(ol_ops, 1, "# operations per a online tx.");      // NOLINT
DEFINE_uint64(ol_rratio, 100, "rate of reads in a online tx."); // NOLINT
DEFINE_uint64(ol_wp_wratio, 0,                                  // NOLINT
              "Probability that online processing goes to write the WP target "
              "area.");
DEFINE_double(ol_skew, 0.0, "access skew of online tx."); // NOLINT
DEFINE_uint64(ol_thread, 1, "# online worker threads.");  // NOLINT

/**
 * about batch tx
 */
DEFINE_uint64(bt_ops, 1, "# operations per a batch tx.");      // NOLINT
DEFINE_uint64(bt_rratio, 100, "rate of reads in a batch tx."); // NOLINT
DEFINE_double(bt_skew, 0.0, "access skew of batch tx.");       // NOLINT
DEFINE_uint64(bt_thread, 1, "# batch worker threads.");        // NOLINT

bool isReady(const std::vector<char>& readys) { // NOLINT
    for (const char& b : readys) {              // NOLINT
        if (loadAcquire(b) == 0) return false;
    }
    return true;
}

void waitForReady(const std::vector<char>& readys) {
    while (!isReady(readys)) { _mm_pause(); }
}

void worker(const std::size_t thid, const bool is_ol, char& ready,
            const bool& start, const bool& quit, simple_result& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, is_ol ? FLAGS_ol_skew : FLAGS_bt_skew, FLAGS_rec);

    setThreadAffinity(
            static_cast<const int>(is_ol ? thid + FLAGS_bt_thread : thid));

    Token token{};
    Storage storage{is_ol ? get_ol_storages()[thid] : get_bt_storages()[thid]};
    std::vector<shirakami::opr_obj> opr_set;
    opr_set.reserve(is_ol ? FLAGS_ol_ops : FLAGS_bt_ops);
    while (Status::OK != enter(token)) { _mm_pause(); }

    std::size_t ct_abort{0};
    std::size_t ct_commit{0};

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        gen_tx_rw(opr_set, FLAGS_key_len, FLAGS_rec,
                  is_ol ? FLAGS_ol_ops : FLAGS_bt_ops,
                  is_ol ? FLAGS_ol_rratio : FLAGS_bt_rratio, rnd, zipf);

        if (!is_ol) {
            tx_begin({token, // NOLINT
                      transaction_options::transaction_type::LONG,
                      {get_bt_storages()[thid]}});
        }

    RETRY: // for wp premature // NOLINT
        bool need_verify = true;
        for (auto&& itr : opr_set) {
            Status rc{};
            if (itr.get_type() == OP_TYPE::SEARCH) {
                for (;;) {
                    std::string vb{};
                    rc = search_key(token, storage, itr.get_key(), vb);
                    if (!is_ol && rc == Status::WARN_PREMATURE) {
                        goto RETRY; // NOLINT
                    }
                    if (rc == Status::WARN_NOT_FOUND) { LOG(FATAL); }
                    if (rc == Status::OK || rc == Status::ERR_VALIDATION) {
                        break;
                    }
                }
            } else if (itr.get_type() == OP_TYPE::UPDATE) {
                Storage target_st{storage};

                // whether ol write from batch table
                if (is_ol &&
                    FLAGS_ol_wp_wratio > (rnd.next() % 100)) { // NOLINT
                    target_st =
                            get_bt_storages().at(rnd.next() % FLAGS_bt_thread);
                }

                // update function is not implemented yet.
                rc = upsert(token, target_st, itr.get_key(),
                            std::string(FLAGS_val_len, '0'));
                if (!is_ol && rc == Status::WARN_PREMATURE) {
                    goto RETRY; // NOLINT
                }
                if (rc == Status::WARN_INVALID_ARGS ||
                    rc == Status::ERR_PHANTOM ||
                    rc == Status::WARN_INVALID_HANDLE ||
                    rc == Status::WARN_CONCURRENT_INSERT) {
                    LOG(FATAL) << "ec: " << rc << std::endl;
                }
            } else {
                LOG(FATAL) << "unkown operation";
            }
            if (rc == Status::ERR_VALIDATION) {
                ++ct_abort;
                abort(token);
                need_verify = false;
                goto RETRY; // NOLINT
                // for occ
                // Without it, OCC can commit well.
                // Because if the transaction changes the access destination, the collision with the batch may be avoided.
            }
        }

        if (need_verify) {
            if (commit(token) == Status::OK) { // NOLINT
                ++ct_commit;
            } else {
                ++ct_abort;
                abort(token);
            }
        }
    }
    leave(token);

    res.set_ct_abort(ct_abort);
    res.set_ct_commit(ct_commit);
}

void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res_ol(
            FLAGS_ol_thread); // NOLINT
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res_bt(
            FLAGS_bt_thread); // NOLINT

    std::vector<char> readys(FLAGS_ol_thread + FLAGS_bt_thread); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < FLAGS_bt_thread; ++i) {
        thv.emplace_back(worker, i, false, std::ref(readys[i]), std::ref(start),
                         std::ref(quit), std::ref(res_bt[i]));
    }

    for (std::size_t i = 0; i < FLAGS_ol_thread; ++i) {
        thv.emplace_back(worker, i, true, std::ref(readys[i + FLAGS_bt_thread]),
                         std::ref(start), std::ref(quit), std::ref(res_ol[i]));
    }

    waitForReady(readys);
    LOG(INFO) << "start ycsb exp.";
    storeRelease(start, true);
#if 0
    for (size_t i = 0; i < FLAGS_duration; ++i) {
        sleepMs(1000);  // NOLINT
    }
#else
    if (sleep(FLAGS_duration) != 0) { LOG(FATAL) << "sleep error."; }
#endif
    storeRelease(quit, true);
    LOG(INFO) << "stop ycsb exp.";
    for (auto& th : thv) th.join();

    output_result(res_ol, res_bt);
    LOG(INFO) << "end experiments, start cleanup.";
}

void init_google_logging() {
    google::InitGoogleLogging("shirakami-bench-bcc_3b_st");
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
