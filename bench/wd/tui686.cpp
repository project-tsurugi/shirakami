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

#include <chrono>
#include <cstring>
#include <thread>

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

#include "glog/logging.h"

using namespace shirakami;

/**
 * general option.
 */
DEFINE_uint64(duration, 4, "Duration of benchmark in seconds."); // NOLINT
DEFINE_uint64(records, 100, "# of records per transaction.");    // NOLINT
DEFINE_uint64(threads, 8, "# worker threads.");                  // NOLINT

static bool isReady(const std::vector<char>& readys); // NOLINT
static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

static void worker(size_t thid, char& ready, const bool& start,
                   const bool& quit, std::atomic<std::size_t>& res, Storage st);

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> res{0}; // NOLINT

    std::vector<char> readys(FLAGS_threads); // NOLINT
    std::vector<std::thread> thv;

    Storage st{};
    auto rc = create_storage("", st);
    CHECK(rc == Status::OK);

    for (std::size_t i = 0; i < FLAGS_threads; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                         std::ref(quit), std::ref(res), st);
    }
    waitForReady(readys);
    storeRelease(start, true);

    if (sleep(FLAGS_duration) != 0) {
        LOG(ERROR) << log_location_prefix << "sleep error.";
    }

    storeRelease(quit, true);
    for (auto& th : thv) th.join();

    LOG(INFO) << "result " << res;
    LOG(INFO) << "result[/s] " << res / FLAGS_duration;
    LOG(INFO) << "similar result for sbench " << res * (20 / FLAGS_duration);
}

int main(int argc, char* argv[]) try { // NOLINT
    google::InitGoogleLogging("shirakami-bench-tui686");
    gflags::SetUsageMessage(static_cast<const std::string&>(
            "tui686 benchmark for shirakami")); // NOLINT
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_stderrthreshold = 0; // to display info log

    database_options options{};
    options.set_epoch_time(3000);
    init(options); // NOLINT
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
            const bool& quit, std::atomic<std::size_t>& res, Storage st) {
    // prepare
    setThreadAffinity(static_cast<const int>(thid));

    // send signal for start
    storeRelease(ready, 1);

    // wait sync
    while (!loadAcquire(start)) _mm_pause();

    // bench
    auto k = thid;
    std::uint64_t score{0};
    std::chrono::nanoseconds waited{};
    while (likely(!loadAcquire(quit))) {
        auto commit_and_wait = [](Token t) {
            auto commit_rc = commit(t);
            if (commit_rc == Status::OK) {
                return static_cast<std::chrono::nanoseconds>(0);
            }
            CHECK(commit_rc == Status::WARN_WAITING_FOR_OTHER_TX);
            auto s_ts = std::chrono::system_clock::system_clock::now();
            do {
                _mm_pause();
            } while ((commit_rc = check_commit(t)) ==
                     Status::WARN_WAITING_FOR_OTHER_TX);
            auto e_ts = std::chrono::system_clock::system_clock::now();
            CHECK(commit_rc == Status::OK);
            return e_ts - s_ts;
        };
        auto put = [st](Token t, std::uint64_t key) {
            // omit something
            std::string_view keystr{reinterpret_cast<char*>(&key), 8};
            CHECK(Status::OK == insert(t, st, keystr, keystr));
        };
        auto wait_start_tx = [](Token t) {
            TxStateHandle sth{};
            CHECK(acquire_tx_state_handle(t, sth) == Status::OK);
            for (;;) {
                TxState state;
                CHECK(check_tx_state(sth, state) == Status::OK);
                if (state.state_kind() == TxState::StateKind::STARTED) {
                    break;
                }
                CHECK(state.state_kind() == TxState::StateKind::WAITING_START);
                _mm_pause();
            }
        };
        auto insert_tx_work = [st, wait_start_tx, put, commit_and_wait](
                                      std::uint64_t& k, std::uint64_t delta,
                                      std::chrono::nanoseconds& waited) {
            Token t{};
            CHECK(Status::OK == enter(t));
            CHECK(Status::OK ==
                  tx_begin({t,
                            transaction_options::transaction_type::LONG,
                            {st}}));
            wait_start_tx(t);
            for (std::uint64_t i = 0; i < FLAGS_records; ++i) {
                put(t, k);
                k += delta;
            }
            auto wait_duration = commit_and_wait(t);
            CHECK(leave(t) == Status::OK);
            waited += wait_duration;
            return;
        };
        // thread work
        insert_tx_work(k, FLAGS_threads, waited);
        score += FLAGS_records;
    }
    LOG(INFO) << "done thread " << thid << ", score " << score << ", waited "
              << waited.count() << "[ns]";
    res += score;
}
