
#include <iostream>
#include <vector>

#include "declare_gflags.h"
#include "param.h"

#include "memory.h"

#include "concurrency_control/wp/include/garbage.h"

#include "glog/logging.h"

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_th >= 1) {
        std::cout << "FLAGS_th:\t" << FLAGS_th << std::endl;
    } else {
        LOG(FATAL) << "error about setting FLAGS_th.";
    }
    if (FLAGS_d >= 1) {
        printf("FLAGS_d : %zu\n", FLAGS_d); // NOLINT
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }

    printf("Fin check_flags()\n"); // NOLINT
}

void output_result() {
    std::cout << "gc_val[/s]:\t"
              << shirakami::garbage::gc_handle::get_gc_ct_val().load(
                         std::memory_order_acquire) /
                         FLAGS_d
              << std::endl;
    std::cout << "gc_ver[/s]:\t"
              << shirakami::garbage::gc_handle::get_gc_ct_ver().load(
                         std::memory_order_acquire) /
                         FLAGS_d
              << std::endl;
    shirakami::displayRusageRUMaxrss();
}
