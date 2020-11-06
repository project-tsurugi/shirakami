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

#include "msgpack-c/include/msgpack.hpp"

namespace shirakami::cpr {

inline std::atomic<bool> kCheckPointThreadEnd{false};
inline std::thread kCheckPointThread;

enum class phase : char {
    REST = 0,
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
    phase get_phase() { return phase_version_.load(std::memory_order_acquire).get_phase(); } // NOLINT

    std::uint64_t get_version() { return phase_version_.load(std::memory_order_acquire).get_version(); } // NOLINT

    std::size_t get_max_version() { return max_version_; }

    void set_phase_version(phase_version new_phase_version) {
        phase_version_.store(new_phase_version, std::memory_order_release);
    }

    void set_max_version(std::size_t num) { max_version_ = num; }

private:
    std::atomic<phase_version> phase_version_{};
    /**
     * @brief max version number of read/write set.
     * @details this number means the tx depends on at most version @a max_version_.
     * So if global version is larger than @a max_version_, the transactions which has @a max_version_ less than global
     * version is durable.
     */
    std::size_t max_version_{0};
};

class log_record {
public:
    log_record() = default;

    log_record(std::string_view key, std::string_view val) {
        key_ = key;
        val_ = val;
    }

    std::string_view get_key() { return key_; }

    std::string_view get_val() { return val_; }

    MSGPACK_DEFINE (key_, val_);
private:
    std::string key_;
    std::string val_;
};

class log_records {
public:
    void emplace_back(std::string_view key, std::string_view val) {
        vec_.emplace_back(key, val);
    }

    std::vector<log_record> &get_vec() { return vec_; } // NOLINT

    MSGPACK_DEFINE (vec_);
private:
    std::vector<log_record> vec_;
};

/**
 * @brief This is checkpoint thread and manager of cpr.
 */
extern void checkpoint_thread();

/**
 * @brief Checkpointing for entire database.
 */
extern void checkpointing();

[[maybe_unused]] static void invoke_checkpoint_thread() {
    kCheckPointThreadEnd.store(false, std::memory_order_release);
    kCheckPointThread = std::thread(checkpoint_thread);
}

[[maybe_unused]] static void join_checkpoint_thread() { kCheckPointThread.join(); }

[[maybe_unused]] static void set_checkpoint_thread_end(const bool tf) {
    kCheckPointThreadEnd.store(tf, std::memory_order_release);
}

}  // namespace shirakami::cpr
