
#include <stdint.h>
#include <stdio.h>
#include <iostream>
#include <vector>
#include <string_view>

#include "declare_gflags.h"
#include "simple_result.h"
#include "memory.h"
#include "shirakami/logging.h"
#include "glog/logging.h"

using namespace shirakami;

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_cr >= 0 && FLAGS_cr <= 100) { // NOLINT
        std::cout << "FLAGS_cr:\t" << FLAGS_cr << std::endl;
    } else {
        LOG(ERROR) << log_location_prefix << "error about setting FLAGS_cr.";
    }
    if (FLAGS_d >= 1) {
        printf("FLAGS_d : %zu\n", FLAGS_d); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Duration of benchmark in seconds must be larger than 0.";
    }

    printf("Fin check_flags()\n"); // NOLINT
}

void output_result(std::vector<simple_result> const& res) {
    std::uint64_t ol_ct_commit{0};
    for (auto&& elem : res) { ol_ct_commit += elem.get_ct_commit(); }
    std::cout << "ol_throughput[tps]:\t" << ol_ct_commit / FLAGS_d << std::endl;

    shirakami::displayRusageRUMaxrss();
}
