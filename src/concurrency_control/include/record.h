/**
 * @file concurrency_control/include/record.h
 * @brief header about record
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <cstdint>
#include <string>
#include <string_view>

#include "concurrency_control/include/read_by.h"
#include "concurrency_control/include/tid.h"

#include "atomic_wrapper.h"
#include "cpu.h"
#include "version.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record() = default;

    ~Record();

    /**
     * @brief ctor.
     * @details This is used for creating page with value at recovery logic.
     */
    Record(std::string_view key, std::string_view val);

    explicit Record(std::string_view key);

    Record(tid_word const& tidw, std::string_view vinfo) : tidw_(tidw) {
        latest_.store(new version(vinfo), std::memory_order_release); // NOLINT
    }

    // start: getter
    void get_key(std::string& out) { out = key_; }

    [[nodiscard]] std::string* get_key_ptr() { return &key_; }

    [[nodiscard]] std::string_view get_key_view() const { return key_; }

    [[nodiscard]] version* get_latest() const {
        return latest_.load(std::memory_order_acquire);
    }

    point_read_by_short& get_read_by() { return read_by_; }

    [[nodiscard]] tid_word get_stable_tidw();

    [[nodiscard]] tid_word get_tidw() const { return tidw_; }

    tid_word& get_tidw_ref() { return tidw_; }

    [[nodiscard]] tid_word const& get_tidw_ref() const { return tidw_; }

    std::shared_mutex& get_mtx_value() { return mtx_value_; }

    void get_value(std::string& out) {
        std::shared_lock<std::shared_mutex> lock{get_mtx_value()};
        get_latest()->get_value(out);
    }

    point_read_by_long& get_point_read_by_long() { return point_read_by_long_; }

    std::atomic<std::size_t>& get_shared_tombstone_count() {
        return shared_tombstone_count_;
    }

    // end: getter
    void lock() { tidw_.lock(); }

    void reset_ts() { tidw_.reset(); }

    void set_latest(version* const ver) {
        latest_.store(ver, std::memory_order_release);
    }

    void set_tid(tid_word const& tid) {
        storeRelease(tidw_.get_obj(), tid.get_obj());
    }

    void set_value(std::string_view const v) {
        std::lock_guard<std::shared_mutex> lock{get_mtx_value()};
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

    // read information about long transaction
    /**
     * @brief read information about point read by long transaction.
     */
    point_read_by_long point_read_by_long_{};
    // ==========

    /**
     * @brief The count about shared tombstone.
     */
    std::atomic<std::size_t> shared_tombstone_count_{0};
};

} // namespace shirakami
