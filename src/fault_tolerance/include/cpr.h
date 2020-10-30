/**
 * @file cpr.h
 * @details cpr is concurrent prefix recovery
 * ( https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf ).
 */

#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <tuple>

#ifdef CC_SILO_VARIANT

#include "concurrency_control/silo_variant/include/epoch.h"

#endif

namespace shirakami::cpr {

inline std::atomic<bool> kCheckPointThreadEnd{false};
inline std::thread kCheckPointThread;

enum class phase : char {
    REST = 0,
    PREPARE,
    IN_PROGRESS,
    WAIT_FLUSH,
};

class phase_version {
public:
    phase_version() : phase_{phase::REST}, version_{0} {}

    phase get_phase() { return phase_; }

    std::uint64_t get_version() { return version_; }

    void inc_version() { version_ += 1; }

    void set_phase(phase new_phase) { phase_ = new_phase; }

    void set_version(std::uint64_t new_version) { version_ = new_version; }

private:
    phase phase_: 8;
    std::uint64_t version_: 56;
};

class global_phase_version {
public:
    static phase_version get_gpv() { return body.load(std::memory_order_acquire); }

    static void inc_version() {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.inc_version();
        body.store(new_body, std::memory_order_release);
    }

    static void init() {
        body.store(phase_version(), std::memory_order_release);
    }

    static void set_gp(phase new_phase) {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.set_phase(new_phase);
        body.store(new_body, std::memory_order_release);
    }

    static void set_rest() {
        phase_version new_body = body.load(std::memory_order_acquire);
        new_body.set_phase(phase::REST);
        new_body.set_version(new_body.get_version() + 1);
        body.store(new_body, std::memory_order_release);
    }

private:
    static inline std::atomic<phase_version> body{phase_version()};
};

/**
 * @brief This object is had by worker thread for concurrent prefix recovery.
 */
class cpr_local_handler {
public:
    phase get_phase() { return phase_version_.get_phase(); } // NOLINT

    std::uint64_t get_version() { return phase_version_.get_version(); } // NOLINT

    void set_phase_version(phase_version new_phase_version) {
        phase_version_ = new_phase_version;
    }

private:
    phase_version phase_version_{};
};

/**
 * @brief This is checkpoint thread and manager of cpr.
 */
extern void checkpoint_thread();

static void invoke_checkpoint_thread() {
    kCheckPointThreadEnd.store(false, std::memory_order_release);
    kCheckPointThread = std::thread(checkpoint_thread);
}

static void join_checkpoint_thread() { kCheckPointThread.join(); }

static void set_checkpoint_thread_end(const bool tf) {
    kCheckPointThreadEnd.store(tf, std::memory_order_release);
}

}  // namespace shirakami::cpr
