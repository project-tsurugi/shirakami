
#include <iostream>
#include <vector>

#include "declare_gflags.h"
#include "param.h"
#include "simple_result.h"

#include "memory.h"

#include "glog/logging.h"

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_cr >= 0 && FLAGS_cr <= 100) { // NOLINT
        std::cout << "FLAGS_cr:\t" << FLAGS_cr << std::endl;
    } else {
        LOG(FATAL) << "error about setting FLAGS_cr.";
    }
    if (FLAGS_d >= 1) {
        printf("FLAGS_d : %zu\n", FLAGS_d); // NOLINT
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }

    printf("Fin check_flags()\n"); // NOLINT
}

void output_result(std::vector<simple_result> const& res) {
    std::uint64_t ol_ct_commit{0};
    for (auto&& elem : res) { ol_ct_commit += elem.get_ct_commit(); }
    std::cout << "ol_throughput[tps]:\t" << ol_ct_commit / FLAGS_d << std::endl;

    shirakami::displayRusageRUMaxrss();
}