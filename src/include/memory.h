/**
 * @file src/include/memory.h
 */

#pragma once

#include <sys/resource.h>

#include <cstdio>
#include <ctime>
#include <iostream>

#include "glog/logging.h"

namespace shirakami {

[[maybe_unused]] static std::size_t getRusageRUMaxrss() {
    struct rusage r {};
    if (getrusage(RUSAGE_SELF, &r) != 0) {
        LOG_FIRST_N(ERROR, 1) << "getrusage error";
    }
    return r.ru_maxrss; // LINT
}

[[maybe_unused]] static void displayRusageRUMaxrss() { // LINT
    struct rusage r {};
    if (getrusage(RUSAGE_SELF, &r) != 0) {
        LOG_FIRST_N(ERROR, 1) << "getrusage error.";
        return;
    }
    std::size_t maxrss{getRusageRUMaxrss()};
    printf("maxrss:\t%ld kB\n", maxrss); // LINT
}

} // namespace shirakami
