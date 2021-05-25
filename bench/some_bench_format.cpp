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

#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "random.h"

#include "gflags/gflags.h"

#include "glog/logging.h"

using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64(                                                           // NOLINT
        cpumhz, 2000,                                                    // NOLINT
        "# cpu MHz of execution environment. It is used measuring some " // NOLINT
        "time.");                                                        // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");         // NOLINT
DEFINE_uint64(thread, 1, "# worker threads.");                           // NOLINT

/**
 * special option.
 */

static bool isReady(const std::vector<char>& readys); // NOLINT
static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

static void worker(size_t thid, char& ready, const bool& start, const bool& quit, std::uint64_t& res);

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<std::uint64_t> res(FLAGS_thread); // NOLINT

    std::vector<char> readys(FLAGS_thread); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < FLAGS_thread; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res.at(i)));
    }
    waitForReady(readys);
    LOG(INFO) << "start rocksdb exp.";
    storeRelease(start, true);
    for (size_t i = 0; i < FLAGS_duration; ++i) {
        sleepMs(1000); // NOLINT
    }
    storeRelease(quit, true);
    LOG(INFO) << "stop rocksdb exp.";
    for (auto& th : thv) th.join();

    std::uint64_t sum{0};
    for (auto&& elem : res) {
        sum += elem;
    }
    SPDLOG_INFO("Throughput: {0} /s", sum / FLAGS_duration);
}

static void load_flags() {
    if (FLAGS_thread >= 1) {
        LOG(INFO) << "FLAGS_thread : {0}", FLAGS_thread;
    } else {
        LOG(FATAL) << "Number of threads must be larger than 0.";
        exit(1);
    }
    if (FLAGS_cpumhz > 1) {
        LOG(INFO) << "FLAGS_cpumhz : {0}", FLAGS_cpumhz;
    } else {
        LOG(FATAL) << "CPU MHz of execution environment. It is used measuring some time. It must be larger than 0.";
        exit(1);
    }
    if (FLAGS_duration >= 1) {
        LOG(INFO) << "FLAGS_duration : {0}", FLAGS_duration;
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
        exit(1);
    }
    LOG(INFO) << "Fin load_flags()";
}

int main(int argc, char* argv[]) { // NOLINT
    google::InitGoogleLogging("shirakami-bench-some_bench_format");
    gflags::SetUsageMessage(static_cast<const std::string&>("RocksDB benchmark")); // NOLINT
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    load_flags();

    invoke_leader();
    LOG(INFO) << "Fin invoke_leader";

    return 0;
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
            const bool& quit, std::uint64_t& res) {
    // init work
    Xoroshiro128Plus rnd;
    std::uint64_t sum{0};

    // this function can be used in Linux environment only.
    setThreadAffinity(static_cast<const int>(thid));

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
    }
    res = sum;
}
