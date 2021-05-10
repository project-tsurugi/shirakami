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
#include "./include/gen_key.h"

// shirakami-impl interface library
#include "clock.h"
#include "logger.h"
#include "random.h"
#include "tuple_local.h"

#include "shirakami/interface.h"

using namespace spdlog;
using namespace shirakami::logger;

namespace shirakami {

size_t decideParallelBuildNumber(const std::size_t record) { // NOLINT
    // if table size is very small, it builds by single thread.
    if (record < 1000) return 1; // NOLINT

    // else
    return std::thread::hardware_concurrency();
}

void parallel_build_db(const std::size_t start, const std::size_t end, const std::size_t key_length,
                       const std::size_t value_length) {
    Xoroshiro128Plus rnd;
    Token token{};
    enter(token);

    tx_begin(token); // NOLINT

    for (uint64_t i = start; i <= end; ++i) {
        auto ret = insert(token, storage, make_key(key_length, i), std::string(value_length, '0'));
        assert(ret == Status::OK); // NOLINT
    }
    auto ret = commit(token);
    assert(ret == Status::OK); // NOLINT
    leave(token);
}

void build_db(const std::size_t record, const std::size_t key_length, const std::size_t value_length) {
    register_storage(storage);
    std::vector<std::thread> thv;

    size_t max_thread{decideParallelBuildNumber(record)};
    for (size_t i = 0; i < max_thread; ++i) {
        thv.emplace_back(parallel_build_db, i * (record / max_thread), i != max_thread - 1 ? (i + 1) * (record / max_thread) - 1 : record - 1, key_length, value_length);
    }

    for (auto& th : thv) th.join();
}

} // namespace shirakami
