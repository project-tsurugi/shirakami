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

// shirakami/src/include/
#include "scheme.hh"

// shirakami/include/
#include "kvs/scheme.h"

size_t decideParallelBuildNumber(std::size_t record,  // NOLINT
                                 std::size_t thread);
void parallel_build_mtdb(std::size_t start, std::size_t end,
                         std::size_t value_length);
void build_mtdb(std::size_t record, std::size_t thread,
                std::size_t value_length);
