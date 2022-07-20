/**
 * @file concurrency_control/wp/include/record.h
 * @brief header about record
 */

#pragma once

#include <mutex>
#include <shared_mutex>
#include <string_view>

#include "concurrency_control/wp/include/read_by.h"
#include "concurrency_control/wp/include/tid.h"

#include "atomic_wrapper.h"
#include "cpu.h"
#include "version.h"

#include "glog/logging.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record() = default;

    ~Record();

    /**
     * @brief ctor.
     * @details This is used for insert logic.
     * todo delete
     */
    Record(std::string_view key, std::string_view val);

    explicit Record(std::string_view key);

    Record(tid_word const& tidw, std::string_view vinfo) : tidw_(tidw) {
        latest_.store(new version(vinfo), std::memory_order_release); // NOLINT
    }

    void get_key(std::string& out) { out = key_; }

    [[nodiscard]] std::string* get_key_ptr() { return &key_; }

    std::string_view get_key_view() { return key_; }

    [[nodiscard]] version* get_latest() const {
        return latest_.load(std::memory_order_acquire);
    }

    std::mutex& get_lk_for_gc() { return lk_for_gc_; }

    point_read_by_short& get_read_by() { return read_by_; }

    [[nodiscard]] tid_word get_stable_tidw();

    [[nodiscard]] tid_word get_tidw() const { return tidw_; }

    tid_word& get_tidw_ref() { return tidw_; }

    [[nodiscard]] tid_word const& get_tidw_ref() const { return tidw_; }

    void get_value(std::string& out) {
        std::shared_lock<std::shared_mutex> lock{mtx_value_};
        get_latest()->get_value(out);
    }

    void lock() { tidw_.lock(); }

    void reset_ts() { tidw_.reset(); }

    void set_latest(version* const ver) {
        latest_.store(ver, std::memory_order_release);
    }

    void set_tid(tid_word const& tid) {
        storeRelease(tidw_.get_obj(), tid.get_obj());
    }

    void set_value(std::string_view const v) {
        std::lock_guard<std::shared_mutex> lock{mtx_value_};
        get_latest()->set_value(v);
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

    std::shared_mutex mtx_value_{};

    point_read_by_short read_by_{};

    std::mutex lk_for_gc_{};
};

} // namespace shirakami
