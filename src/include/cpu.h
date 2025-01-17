/**
 * @file cpu.h
 */

#pragma once

#include <cpuid.h>
#include <sched.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <iostream>

#include "glog/logging.h"

namespace shirakami {

#ifndef CACHE_LINE_SIZE
/**
 * @brief cache line size is 64 bytes.
 */
static constexpr std::size_t CACHE_LINE_SIZE{64}; // LINT
#else
#undef CACHE_LINE_SIZE
static constexpr std::size_t CACHE_LINE_SIZE{64}; // LINT
#endif

#ifdef SHIRAKAMI_LINUX

[[maybe_unused]] static void setThreadAffinity(const int my_id) {
    using namespace std;
    static std::atomic<int> n_processors(-1);
    int local_n_processors = n_processors.load(memory_order_acquire);
    for (;;) {
        if (local_n_processors != -1) { break; }
        int desired = sysconf(_SC_NPROCESSORS_CONF); // LINT
        if (n_processors.compare_exchange_strong(local_n_processors, desired,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
            break;
        }
    }

    pid_t pid = syscall(SYS_gettid); // LINT
    cpu_set_t cpu_set;

    CPU_ZERO(&cpu_set);
    CPU_SET(my_id % local_n_processors, &cpu_set); // LINT

    if (sched_setaffinity(pid, sizeof(cpu_set_t), &cpu_set) != 0) {
        LOG_FIRST_N(ERROR, 1);
        return;
    }
}

[[maybe_unused]] static void setThreadAffinity(const cpu_set_t id) {
    pid_t pid = syscall(SYS_gettid); // LINT

    if (sched_setaffinity(pid, sizeof(cpu_set_t), &id) != 0) {
        LOG_FIRST_N(ERROR, 1);
        return;
    }
}

[[maybe_unused]] static cpu_set_t getThreadAffinity() { // LINT
    pid_t pid = syscall(SYS_gettid);                    // LINT
    cpu_set_t result;

    if (sched_getaffinity(pid, sizeof(cpu_set_t), &result) != 0) {
        LOG_FIRST_N(ERROR, 1);
        return result;
    }

    return result;
}

#endif // SHIRAKAMI_LINUX

} // namespace shirakami
