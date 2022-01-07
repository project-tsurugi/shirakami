
#include <iostream>
#include <vector>

#include "declare_gflags.h"
#include "param.h"
#include "simple_result.h"

#include "memory.h"

#include "glog/logging.h"

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_d >= 1) {
        printf("FLAGS_d:\t%zu\n", FLAGS_d); // NOLINT
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }
    if (0 <= FLAGS_cr && FLAGS_cr <= 100) {
        std::cout << "FLAGS_cr:\t" << FLAGS_cr << std::endl;
    } else {
        LOG(FATAL);
    }
    printf("Fin check_flags()\n"); // NOLINT
}

void output_result(std::vector<simple_result> const& res_bt) {
    std::uint64_t bt_ct_commit{0};
    for (auto&& elem : res_bt) { bt_ct_commit += elem.get_ct_commit(); }
    std::cout << "bt_throughput[tps]:\t" << bt_ct_commit / FLAGS_d
              << std::endl;
    std::cout << "bt_throughput[ops/s]:\t"
              << (bt_ct_commit * tx_size) / FLAGS_d << std::endl;

    shirakami::displayRusageRUMaxrss();
}
