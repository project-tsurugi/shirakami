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

#pragma once

#include <utility>
#include <vector>

#include "cpu.h"

#include "shirakami/storage_options.h"

namespace shirakami {

/**
 * @brief One storage for ycsb experiments.
 */
inline Storage storage;

/**
 * @brief For ycsb_mb_nc.cpp
 * @details If this is true, All worker threads access separate storage. 
 * Therefore, there is no conflict.
 */
inline bool use_separate_storage{false};

/**
 * @brief For ycsb_mb_nc.cpp
 * @details If this is true, All worker threads access separate storage. 
 * Therefore, there is no conflict.
 */
alignas(CACHE_LINE_SIZE) inline std::vector<
        Storage> separate_storage{}; // NOLINT

/**
 * global variables getter / setter
 */

inline std::vector<Storage>& get_separate_storage() { return separate_storage; }

inline bool get_use_separate_storage() { return use_separate_storage; }

inline void set_use_separate_storage(bool tf) { use_separate_storage = tf; }

/**
 * Other functions.
 */

void build_db(std::size_t record, std::size_t key_length,
              std::size_t value_length, std::size_t threads);

/**
 * @brief Determine the number of parallels to use for the build.
 */
size_t decideParallelBuildNumber(std::size_t record); // NOLINT

void parallel_build_db(std::size_t start, std::size_t end,
                       std::size_t key_length, std::size_t value_length,
                       std::size_t pbd_storage);

} // namespace shirakami
