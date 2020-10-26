/**
 * @file cpr.h
 * @details cpr is concurrent prefix recovery
 * ( https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf ).
 */

#pragma once

#include <atomic>

#include "fileio.h"

#include "kvs/interface.h"
#include "kvs/scheme.h"

namespace shirakami::cpr {

enum class phase : char {
    REST,
    PREPARE,
    IN_PROGRESS,
    WAIT_FLUSH,
};

class phase_version {
public:
    phase get_phase() { return phase_; }

    std::uint64_t get_version() { return version_; }

    void inc_version() { version_ += 1; }

    void set_phase(phase new_phase) { phase_ = new_phase; }

    void set_version(std::uint64_t new_version) { version_ = new_version; }

private:
    phase phase_: 1{phase::REST};
    std::uint64_t version_: 63{0};
};

class global_phase_version {
public:
    phase_version get_gpv() { return body.load(std::memory_order_acquire); }

    void inc_version() {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.inc_version();
        body.store(new_body, std::memory_order_release);
    }

    void init() {
        body.store(phase_version(), std::memory_order_release);
    }

    void set_gp(phase new_phase) {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.set_phase(new_phase);
        body.store(new_body, std::memory_order_release);
    }

private:
    static inline std::atomic<phase_version> body;
};

}  // namespace shirakami::cpr
