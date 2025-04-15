/**
 * @file concurrency_control/include/version.h
 */

#pragma once

#include <atomic>
#include <shared_mutex>
#include <string_view>

#include "cpu.h"

#include "concurrency_control/include/tid.h"

#include "glog/logging.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) version { // NOLINT
public:
    explicit version() { set_next(nullptr); }

    // for newly insert
    explicit version(std::string_view value) {
        set_value(value, false);
        set_next(nullptr);
    }

    // for insert version to version list at latest
    explicit version(std::string_view const value, version* const next) {
        set_value(value, false);
        set_next(next);
    }

    // for insert version to version list at middle
    explicit version(tid_word const& tid, std::string_view const value,
                     version* const next)
        : tid_(tid) {
        set_value(value, false);
        set_next(next);
    }

    [[nodiscard]] version* get_next() const {
        return next_.load(std::memory_order_acquire);
    }

    void get_value(std::string& out, bool do_lock) {
        std::shared_lock<std::shared_mutex> lk{mtx_value_, std::defer_lock};
        if (do_lock) { lk.lock(); }
        out = value_;
    }

    [[nodiscard]] tid_word get_tid() const { return tid_; }

    /**
     * @brief set value
     * @pre This is also for initialization of version.
     */
    void set_value(std::string_view const value, bool do_lock) {
        std::unique_lock<std::shared_mutex> lk{mtx_value_, std::defer_lock};
        if (do_lock) { lk.lock(); }
        value_ = value;
    }

    void set_next(version* const next) {
        next_.store(next, std::memory_order_release);
    }

    void set_tid(tid_word const& tid) { tid_ = tid; }

private:
    tid_word tid_{};

    /**
     * @brief Value data.
     */
    std::string value_;

    /**
     * @brief Mutex for value.
     */
    std::shared_mutex mtx_value_{};

    /**
     * @brief pointer to next version.
     */
    std::atomic<version*> next_{nullptr};
};
} // namespace shirakami
