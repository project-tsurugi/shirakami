/**
 * @file concurrency_control/wp/include/record.h
 * @brief header about record
 */

#pragma once

#include <string_view>

#include "concurrency_control/wp/include/tid.h"

#include "atomic_wrapper.h"
#include "cpu.h"
#include "version.h"

#include "glog/logging.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record() = default;

    ~Record() {
        auto* ver = get_latest();
        while (ver != nullptr) {
            auto* ver_tmp = get_latest()->get_next();
            delete ver; // NOLINT
            ver = ver_tmp;
        }
    }

    /**
     * @brief ctor.
     * @details This is used for insert logic.
     */
    Record(std::string_view const key, std::string_view const val) : key_(key) {
        latest_.store(new version(val), std::memory_order_release); // NOLINT
        tidw_.set_lock(true);
        tidw_.set_latest(true);
        tidw_.set_absent(true);
    }

    Record(tid_word const& tidw, std::string_view vinfo) : tidw_(tidw) {
        latest_.store(new version(vinfo), std::memory_order_release); // NOLINT
    }

    std::string_view get_key() { return key_; }

    [[nodiscard]] std::string* get_key_ptr() { return &key_; }

    [[nodiscard]] version* get_latest() const {
        return latest_.load(std::memory_order_acquire);
    }

    [[nodiscard]] tid_word get_stable_tidw() {
        for (;;) {
            tid_word check{loadAcquire(tidw_.get_obj())};
            if (check.get_lock()) {
                _mm_pause();
            } else {
                return check;
            }
        }
    }

    [[nodiscard]] tid_word get_tidw() const { return tidw_; }

    tid_word& get_tidw_ref() { return tidw_; }

    tid_word const& get_tidw_ref() const { return tidw_; }

    void lock() { tidw_.lock(); }

    void set_tid(tid_word const& tid) {
        storeRelease(tidw_.get_obj(), tid.get_obj());
    }

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

    std::string key_{};
};

} // namespace shirakami
