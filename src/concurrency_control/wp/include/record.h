/**
 * @file concurrency_control/wp/include/record.h
 * @brief header about record
 */

#pragma once

#include "concurrency_control/silo/include/tid.h"

#include "cpu.h"
#include "version.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record(tid_word tid, std::string_view vinfo) : tid_(tid) {
        latest_.store(new version(vinfo), std::memory_order_release);
    }

    void lock() {
        tid_.lock();
    }

    void set_tid(tid_word tid) {
        tid_ = tid;
    }

    void unlock() {
        tid_.unlock();
    }

private:
    tid_word tid_{};

    /**
     * @brief Pointer to latest version
     * @details The version infomation which it should have at each version.
     */
    std::atomic<version*> latest_{nullptr};
};

} // namespace shirakami
