
#include <iostream>
#include <vector>

#include "declare_gflags.h"
#include "param.h"
#include "simple_result.h"

#include "memory.h"

#include "concurrency_control/wp/include/garbage.h"

#include "glog/logging.h"

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_d >= 1) {
        printf("FLAGS_d :\t%zu\n", FLAGS_d); // NOLINT
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }
    std::cout << "FLAGS_skew:\t" << FLAGS_skew << std::endl;
    std::cout << "FLAGS_tx_size:\t" << FLAGS_tx_size << std::endl;

    printf("Fin check_flags()\n"); // NOLINT
}

void output_result(std::vector<simple_result> const& res) {
    std::size_t ct_commit{0};
    for (auto const& elem : res) { ct_commit += elem.get_ct_commit(); }
    std::cout << "throughput[tps]:\t" << ct_commit / FLAGS_d << std::endl;
    shirakami::displayRusageRUMaxrss();
}
