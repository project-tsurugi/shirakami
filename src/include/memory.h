/**
 * @file src/include/memory.h
 */

#pragma once

#include <sys/resource.h>

#include <cstdio>
#include <ctime>
#include <iostream>

namespace shirakami {

[[maybe_unused]] static std::size_t getRusageRUMaxrss() {
    struct rusage r {};
    if (getrusage(RUSAGE_SELF, &r) != 0) {
        std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
                  << std::endl;
        std::abort();
    }
    return r.ru_maxrss; // NOLINT
}

[[maybe_unused]] static void displayRusageRUMaxrss() { // NOLINT
    struct rusage r {};
    if (getrusage(RUSAGE_SELF, &r) != 0) {
        std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
                  << std::endl;
        std::abort();
    }
    std::size_t maxrss{getRusageRUMaxrss()};
    printf("maxrss:\t%ld kB\n", maxrss); // NOLINT
}

} // namespace shirakami
