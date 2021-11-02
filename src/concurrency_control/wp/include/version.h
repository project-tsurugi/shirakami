/**
 * @file concurrency_control/wp/include/version.h
 */

#pragma once

#include <atomic>
#include <string_view>

#include "cpu.h"

#include "concurrency_control/wp/include/tid.h"

#include "glog/logging.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) version { // NOLINT
public:
    explicit version(std::string_view value) { set_value(value); }

    explicit version(tid_word const& tid, std::string_view const value,
                     version* const next)
        : tid_(tid) {
        set_value(value);
        set_next(next);
    }

    ~version() {
        auto* val{get_value()};
        delete val; // NOLINT
    }

    [[nodiscard]] version* get_next() const {
        return next_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string* get_value() const {
        return value_.load(std::memory_order_acquire);
    }

    /**
     * @brief set value
     * @pre This is for initialization of version.
     * If you use in other case, it may occurs std::abort.
     */
    void set_value(std::string_view value) {
        if (get_value() != nullptr) {
            LOG(FATAL) << "usage";
            std::abort();
        }
        value_.store(new std::string(value), // NOLINT
                     std::memory_order_release);
    }

    void set_value(std::string_view new_v, std::string*& old_v) {
        old_v = get_value();
        value_.store(new std::string(new_v), std::memory_order_release); // NOLINT
    }

    void set_next(version* next) {
        next_.store(next, std::memory_order_release);
    }

    void set_tid(tid_word const& tid) { tid_ = tid; }

private:
    tid_word tid_{};

    /**
     * @brief value.
     */
    std::atomic<std::string*> value_{nullptr};
    /**
     * @brief pointer to next version.
     */
    std::atomic<version*> next_{nullptr};
};
} // namespace shirakami