/**
 * @file src/include/tsc.h
 */

#pragma once

#include <cstdint>

namespace shirakami {

[[maybe_unused]] static uint64_t rdtsc() { // LINT
    uint64_t rax{};
    uint64_t rdx{};

    asm volatile("cpuid" ::: "rax", "rbx", "rcx", "rdx"); // LINT
    asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));         // LINT

    return (rdx << 32) | rax; // LINT
}

[[maybe_unused]] static uint64_t rdtscp() { // LINT
    uint64_t rax{};
    uint64_t rdx{};
    uint64_t aux{};

    asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux)::); // LINT

    return (rdx << 32) | rax; // LINT
}

} // namespace shirakami
