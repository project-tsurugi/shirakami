/**
 * @file src/include/tsc.h
 */

#pragma once

#include <cstdint>

namespace shirakami {

#if defined(__x86_64__)

[[maybe_unused]] static uint64_t rdtsc() { // NOLINT
    uint64_t rax{};
    uint64_t rdx{};

    asm volatile("cpuid" ::: "rax", "rbx", "rcx", "rdx"); // NOLINT
    asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));         // NOLINT

    return (rdx << 32) | rax; // NOLINT
}

[[maybe_unused]] static uint64_t rdtscp() { // NOLINT
    uint64_t rax{};
    uint64_t rdx{};
    uint64_t aux{};

    asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux)::); // NOLINT

    return (rdx << 32) | rax; // NOLINT
}

#elif defined(__aarch64__)

// Use the AArch64 virtual system counter as a monotonic timestamp.
[[maybe_unused]] static uint64_t rdtsc() { // NOLINT
    uint64_t v{};
    asm volatile("mrs %0, cntvct_el0" : "=r"(v)); // NOLINT
    return v;
}

// isb ensures the counter is read after preceding instructions complete.
[[maybe_unused]] static uint64_t rdtscp() { // NOLINT
    uint64_t v{};
    asm volatile("isb; mrs %0, cntvct_el0" : "=r"(v)); // NOLINT
    return v;
}

#endif

} // namespace shirakami
