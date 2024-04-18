
#include <cstdio>
#include <iostream>
#include <string_view>

#include "declare_gflags.h"
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
    std::cout << "FLAGS_ops\t" << FLAGS_ops << std::endl;
    printf("Fin check_flags()\n"); // NOLINT
}