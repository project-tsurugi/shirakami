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

#include <emmintrin.h>
#include <unistd.h>
#include <iostream>
#include <cstring>
#include <exception>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// shirakami/bench/bcc_5/include
#include "param.h"
#include "simple_result.h"
#include "storage.h"
#include "utility.h"
// shirakami/bench/include
#include "gen_tx.h"
// shirakami/src/include
#include "atomic_wrapper.h"
#include "compiler.h"
#include "cpu.h"
#include "shirakami/interface.h"
#include "glog/logging.h"
#include "gflags/gflags.h"
#include "random.h"
#include "shirakami/binary_printer.h"
#include "shirakami/logging.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"
#include "zipf.h"

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

void worker(const std::size_t thid, const bool is_ol, char& ready,
            const bool& start, const bool& quit, simple_result& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, 0, rec_num);
    // skew 0

    setThreadAffinity(static_cast<const int>(thid));

    Token token{};
    std::vector<opr_obj> opr_set;
    std::size_t tx_size{is_ol ? ol_tx_size : bt_tx_size};
    opr_set.reserve(tx_size);
    while (Status::OK != enter(token)) { _mm_pause(); }

    std::size_t ct_abort{0};
    std::size_t ct_commit{0};

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        gen_tx_rw(opr_set, key_len, rec_num, tx_size, 0, rnd, zipf);

        // wp
        if (!is_ol && tx_begin({token, // NOLINT
                                transaction_options::transaction_type::LONG,
                                {get_bt_storages().at(0)}}) != Status::OK) {
            LOG_FIRST_N(ERROR, 1);
        }

    RETRY: // NOLINT
        for (auto&& itr : opr_set) {
            Status rc{};
            if (itr.get_type() == OP_TYPE::UPDATE) {
                // update function is not implemented yet.
                Storage st{};
                if (is_ol) {
                    if (FLAGS_cr > (rnd.next() % 100)) { // NOLINT
                        st = get_bt_storages().at(0);
                    } else {
                        st = get_ol_storages().at(thid);
                    }
                } else {
                    st = get_bt_storages().at(0);
                }
                rc = upsert(token, st, itr.get_key(),
                            std::string(val_len, '0'));
                if (rc == Status::WARN_PREMATURE) {
                    goto RETRY; // NOLINT
                }
                if (rc != Status::OK) {
                    LOG_FIRST_N(ERROR, 1)
                            << log_location_prefix << "ec: " << rc << std::endl;
                }
            } else {
                LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unkown operation";
            }
        }

        auto rc{commit(token)}; // NOLINT
        if (rc == Status::OK) {
            ++ct_commit;
        } else if (rc != Status::OK && !is_ol) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path.";
        }
    }

    leave(token);
    if (is_ol) { res.set_ct_commit(ct_commit); }
}

void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<simple_result> res_ol(
            ol_th_size); // NOLINT
    alignas(CACHE_LINE_SIZE) simple_result res_bt;

    std::vector<char> readys(std::thread::hardware_concurrency()); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
        if (i == 0) {
            thv.emplace_back(worker, i, false, std::ref(readys[i]),
                             std::ref(start), std::ref(quit), std::ref(res_bt));
        } else {
            thv.emplace_back(worker, i - 1, true, std::ref(readys[i]),
                             std::ref(start), std::ref(quit),
                             std::ref(res_ol.at(i - 1)));
        }
    }

    waitForReady(readys);
    LOG(INFO) << "start exp.";
    storeRelease(start, true);

    if (sleep(FLAGS_d) != 0) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "sleep error.";
    }

    storeRelease(quit, true);
    LOG(INFO) << "stop exp.";
    for (auto& th : thv) th.join();

    output_result(res_ol);
    LOG(INFO) << "end exp, start cleanup.";
}

void init_google_logging() {
    google::InitGoogleLogging("/tmp/shirakami-bench-bcc_5");
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
