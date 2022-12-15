
#include <iostream>
#include <vector>

#include "declare_gflags.h"
#include "param.h"
#include "simple_result.h"

#include "memory.h"

#include "shirakami/logging.h"

#include "glog/logging.h"

using namespace shirakami;

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_d >= 1) {
        printf("FLAGS_d :\t%zu\n", FLAGS_d); // NOLINT
    } else {
        LOG(ERROR) << log_location_prefix
                   << "Duration of benchmark in seconds must be larger than 0.";
    }
    std::cout << "FLAGS_th\t" << FLAGS_th << std::endl;
    printf("Fin check_flags()\n"); // NOLINT
}

void output_result(std::vector<simple_result> const& res) {
    std::size_t ct_abort{0};
    std::size_t ct_commit{0};
    for (auto const& elem : res) { ct_abort += elem.get_ct_abort(); }
    for (auto const& elem : res) { ct_commit += elem.get_ct_commit(); }
    std::cout << "throughput[tps]:\t" << ct_commit / FLAGS_d << std::endl;
    std::cout << "abort_rate:\t" << ct_abort / (ct_commit + ct_abort)
              << std::endl;
    shirakami::displayRusageRUMaxrss();
}
