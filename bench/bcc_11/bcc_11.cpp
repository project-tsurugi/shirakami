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

// shirakami/bench/bcc_10/include
#include "declare_gflags.h"
#include "utility.h"

// shirakami/bench/include
#include "gen_tx.h"

// shirakami/src/include
#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "boost/filesystem.hpp"


#include "glog/logging.h"

#include "gflags/gflags.h"

/**
 * general option.
 */
DEFINE_uint64(d, 1, "Duration of benchmark in seconds."); // NOLINT
DEFINE_uint64(ops, 10, "write operation / tx.");          // NOLINT

using namespace shirakami;

void worker(std::atomic<bool>& quit) {
    // create storage
    Storage st{};
    if (create_storage("", st) != Status::OK) { LOG(FATAL); }

    // prepare token
    Token token{};
    while (Status::OK != enter(token)) { _mm_pause(); }

    // prepare result
    std::size_t ct_commit{0};

    // loop exp
    while (!quit.load(std::memory_order_acquire)) {
        for (std::size_t i = 0; i < FLAGS_ops; ++i) {
            std::string_view k{reinterpret_cast<char*>(&i), // NOLINT
                               sizeof(i)};                  // NOLINT
            auto rc = upsert(token, st, k, "v");
            if (rc != Status::OK) { LOG(FATAL); }
        }
        auto rc{commit(token)}; // NOLINT
        if (rc != Status::OK) { LOG(FATAL); }
        ++ct_commit;
    }

    // output result
    std::uint64_t committed_ops = ct_commit * FLAGS_ops;
    std::uint64_t opsps = committed_ops / FLAGS_d;
    std::cout << "Throughput[ops/s]: " << opsps << std::endl;

    // cleanup
    leave(token);
}

void invoke_leader() {
    std::atomic<bool> quit{false};

    LOG(INFO) << "start exp.";
    std::thread th(worker, std::ref(quit));

    if (sleep(FLAGS_d) != 0) {
        LOG(ERROR) << log_location_prefix << "sleep error.";
    }

    // send signal
    quit.store(true, std::memory_order_release);

    LOG(INFO) << "stop exp.";
    th.join();

    LOG(INFO) << "end exp, start cleanup.";
}

void init_google_logging() {
    google::InitGoogleLogging("/tmp/shirakami-bench-bcc_10");
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
    invoke_leader();
    fin();

    return 0;
} catch (std::exception& e) { std::cerr << e.what() << std::endl; }