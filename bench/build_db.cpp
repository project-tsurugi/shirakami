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

// shirakami/bench
#include "./include/build_db.h"
#include "./include/shirakami_string.h"

// shirakami-impl interface library
#include "concurrency_control/silo_variant/include/interface_helper.h"
#include "clock.h"
#include "logger.h"
#include "random.h"
#include "tuple_local.h"

#include "kvs/interface.h"

namespace shirakami {

#ifdef CC_SILO_VARIANT
using namespace cc_silo_variant;
#endif

size_t decideParallelBuildNumber(const std::size_t record,  // NOLINT
                                 const std::size_t thread) {
    // if table size is very small, it builds by single thread.
    if (record < 1000) return 1;  // NOLINT

    // else
    for (size_t i = thread; i > 0; --i) {
        if (record % i == 0) {
            return i;
        }
        if (i == 1) {
            SPDLOG_DEBUG("fatal error.");
            std::abort();
        }
    }

    return 1;
}

void parallel_build_db(const std::size_t start, const std::size_t end,
                       const std::size_t value_length) {
    Xoroshiro128Plus rnd;
    Token token{};
    enter(token);

#ifdef CC_SILO_VARIANT
    cc_silo_variant::tx_begin(token);
#endif

    for (uint64_t i = start; i <= end; ++i) {
        uint64_t keybs = __builtin_bswap64(i);
        std::string val(value_length, '0');  // NOLINT
        make_string(val, rnd);
        insert(token, {reinterpret_cast<char*>(&keybs), sizeof(uint64_t)}, val); // NOLINT
    }
    commit(token); // NOLINT
    leave(token);
}

void build_db(const std::size_t record, const std::size_t thread,
              const std::size_t value_length) {
    SPDLOG_DEBUG("ycsb::build_mtdb");
    std::vector<std::thread> thv;

    size_t max_thread{decideParallelBuildNumber(record, thread)};
    SPDLOG_DEBUG("start parallel_build_db with {0} threads.", max_thread);
    fflush(stdout);
    for (size_t i = 0; i < max_thread; ++i) {
        thv.emplace_back(parallel_build_db, i * (record / max_thread),  // NOLINT
                         (i + 1) * (record / max_thread) - 1, value_length);
    }

    for (auto &th : thv) th.join();
}

}  // namespace shirakami
