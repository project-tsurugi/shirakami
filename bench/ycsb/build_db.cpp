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

#include <string>

// shirakami/bench
#include "build_db.h"
#include "gen_key.h"

// shirakami-impl interface library
#include "clock.h"
#include "random.h"

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

size_t decideParallelBuildNumber(const std::size_t record) { // NOLINT
    // if table size is very small, it builds by single thread.
    if (record < 1000) return 1; // NOLINT

    // else
    return std::thread::hardware_concurrency();
}

void parallel_build_db(const std::size_t start, const std::size_t end,
                       const std::size_t key_length,
                       const std::size_t value_length,
                       const Storage pbd_storage = 0) { // NOLINT
    Xoroshiro128Plus rnd;
    Token token{};
    auto ret = enter(token);
    if (ret != Status::OK) { LOG_FIRST_N(ERROR, 1) << ret; }

    ret = tx_begin({token}); // NOLINT
    if (ret != Status::OK) { LOG_FIRST_N(ERROR, 1) << ret; }

    std::size_t ctr{0};
    auto* ti = static_cast<session*>(token);
    for (uint64_t i = start; i <= end; ++i) {
        if (!ti->get_tx_began()) {
            ret = tx_begin(
                    {token, transaction_options::transaction_type::SHORT});
            if (ret != Status::OK) { LOG_FIRST_N(ERROR, 1); }
        }
        if (get_use_separate_storage()) {
            ret = insert(token, pbd_storage, make_key(key_length, i),
                         std::string(value_length, '0'));
        } else {
            ret = insert(token, storage, make_key(key_length, i),
                         std::string(value_length, '0'));
        }
        if (ret != Status::OK) { LOG_FIRST_N(ERROR, 1) << ret; }
        ++ctr;
        if (ctr > 10) { // NOLINT
            ret = commit(token);
            if (ret != Status::OK) { LOG_FIRST_N(ERROR, 1) << ret; }
            ctr = 0;
        }
    }
    if (ctr != 0) { ret = commit(token); }
    if (ret != Status::OK) { LOG_FIRST_N(ERROR, 1) << ret; }
    leave(token);
}

void build_db(const std::size_t record, const std::size_t key_length,
              const std::size_t value_length,
              const std::size_t threads = 0) { // NOLINT
    if (get_use_separate_storage()) {
        get_separate_storage().reserve(threads);
        for (std::size_t i = 0; i < threads; ++i) {
            create_storage(std::to_string(i), storage);
            get_separate_storage().emplace_back(storage);

            std::vector<std::thread> thv;
            size_t max_thread{decideParallelBuildNumber(record)};
            for (size_t i = 0; i < max_thread; ++i) {
                thv.emplace_back(parallel_build_db, i * (record / max_thread),
                                 i != max_thread - 1
                                         ? (i + 1) * (record / max_thread) - 1
                                         : record - 1,
                                 key_length, value_length, storage);
            }

            for (auto& th : thv) th.join();
        }
    } else {
        create_storage("", storage);

        std::vector<std::thread> thv;

        size_t max_thread{decideParallelBuildNumber(record)};
        for (size_t i = 0; i < max_thread; ++i) {
            thv.emplace_back(parallel_build_db, i * (record / max_thread),
                             i != max_thread - 1
                                     ? (i + 1) * (record / max_thread) - 1
                                     : record - 1,
                             key_length, value_length, 0);
        }

        for (auto& th : thv) th.join();
    }
}

} // namespace shirakami
