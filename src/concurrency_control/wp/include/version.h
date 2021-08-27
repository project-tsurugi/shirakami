/**
 * @file concurrency_control/wp/include/version.h
 */

#pragma once

#include "cpu.h"

#include "concurrency_control/silo/include/tid.h"

#include <atomic>

namespace shirakami {

class alignas(CACHE_LINE_SIZE) version {
public:
    version(std::string_view vinfo) {
        set_pv(vinfo);
    }

    version* get_next() {
        return next_.load(std::memory_order_acquire);
    }

    std::string* get_pv() {
        return pv_.load(std::memory_order_acquire);
    }

    void set_pv(std::string_view vinfo) {
        pv_.store(new std::string(vinfo), std::memory_order_release);
    }

    /**
     * @brief set pointer to value
     * @details allocate new heap memory for new value, set pointer to the value, and return old value by @a old_v.
     * @param[in] vinfo
     * @param[out] old_v pointer to old value.
     */
    void set_pv(std::string_view vinfo, std::string** old_v) {
        *old_v = get_pv();
        set_pv(vinfo);
    }

    void set_next(version* next) {
        next_.store(next, std::memory_order_release);
    }

private:
    tid_word tid_{};

    /**
     * @brief pointer to value.
     */
    std::atomic<std::string*> pv_{nullptr};
    /**
     * @brief pointer to next version.
     */
    std::atomic<version*> next_{nullptr};
};
} // namespace shirakami