/**
 * @file concurrency_control/wp/include/record.h
 * @brief header about record
 */

#pragma once

#include <string_view>

#include "concurrency_control/wp/include/tid.h"

#include "cpu.h"
#include "version.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record() = default;

    Record(tid_word const& tidw, std::string_view vinfo) : tidw_(tidw) {
        latest_.store(new version(vinfo), std::memory_order_release); // NOLINT
    }

    tid_word& get_tidw_ref() { return tidw_; }

    void lock() { tidw_.lock(); }

    void set_tid(tid_word const& tid) { tidw_ = tid; }

    void unlock() { tidw_.unlock(); }

private:
    /**
     * @brief latest timestamp
     */
    tid_word tidw_{};

    /**
     * @brief Pointer to latest version
     * @details The version infomation which it should have at each version.
     */
    std::atomic<version*> latest_{nullptr};
};

} // namespace shirakami
