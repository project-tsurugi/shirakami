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

#include <algorithm>
#include <unordered_map>

#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "logger.h"
#include "random.h"
#include "tsc.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include <tsl/hopscotch_map.h>

using namespace shirakami;
using namespace shirakami::logger;

/**
 * general option.
 */
DEFINE_uint64(                                                               // NOLINT
        cpumhz, 2100,                                                        // NOLINT
        "# cpu MHz of execution environment. It is used measuring some "     // NOLINT
        "time.");                                                            // NOLINT
DEFINE_uint64(elem_num, 5 * 1000 * 1000, "Insert # elements to some map."); // NOLINT

static void load_flags() {
    if (FLAGS_cpumhz > 1) {
        shirakami_logger->debug("FLAGS_cpumhz : {0}", FLAGS_cpumhz);
    } else {
        shirakami_logger->debug("CPU MHz of execution environment. It is used measuring some time. It must be larger than 0.");
        exit(1);
    }
    shirakami_logger->debug("Fin load_flags()");
}

void std_map_bench(const std::vector<std::uint64_t>& data) {
    std::map<std::uint64_t, std::uint64_t> std_map;
    std::uint64_t begin{rdtscp()};
    for (auto&& elem : data) {
        std_map[elem] = elem;
    }
    std::uint64_t end{rdtscp()};
    shirakami_logger->info("std_map_throughput[ops/us]:\t{0}", FLAGS_elem_num / ((end - begin) / FLAGS_cpumhz / 1000)); // NOLINT
}

void std_unordered_map_bench(const std::vector<std::uint64_t>& data) {
    std::unordered_map<std::uint64_t, std::uint64_t> map;
    std::uint64_t begin{rdtscp()};
    for (auto&& elem : data) {
        map[elem] = elem;
    }
    std::uint64_t end{rdtscp()};
    shirakami_logger->info("std_unordered_map_throughput[ops/us]:\t{0}", FLAGS_elem_num / ((end-begin) /FLAGS_cpumhz / 1000)); // NOLINT
}

void hopscotch_map_bench(const std::vector<std::uint64_t>& data) {
    tsl::hopscotch_map<std::uint64_t, std::uint64_t> map;
    std::uint64_t begin{rdtscp()};
    for (auto&& elem : data) {
        map[elem] = elem;
    }
    std::uint64_t end{rdtscp()};
    shirakami_logger->info("hopscotch_map_throughput[ops/us]:\t{0}", FLAGS_elem_num / ((end-begin) /FLAGS_cpumhz / 1000)); // NOLINT
}

void prepare_data(std::vector<std::uint64_t>& data) {
    data.clear();
    data.reserve(FLAGS_elem_num);
    for (std::size_t i = 0; i < FLAGS_elem_num; ++i) {
        data.emplace_back(i);
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(data.begin(), data.end(), engine);
}

int main(int argc, char* argv[]) { // NOLINT
    logger::setup_spdlog();
    gflags::SetUsageMessage(static_cast<const std::string&>("Map benchmark")); // NOLINT
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    load_flags();


    std::vector<std::uint64_t> data;
    prepare_data(data);
    std_map_bench(data);
    std_unordered_map_bench(data);
    hopscotch_map_bench(data);

    return 0;
}
