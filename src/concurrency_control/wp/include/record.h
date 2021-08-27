/**
 * @file concurrency_control/wp/include/record.h
 * @brief header about record
 */

#pragma once

#include "cpu.h"
#include "lock.h"
#include "version.h"

namespace shirakami {

class alignas(CACHE_LINE_SIZE) Record { // NOLINT
public:
    Record() {} // NOLINT

    Record(std::string_view key, std::string_view val) : tuple_(key, val) {
        // init tidw
        tidw_.set_latest(true);
        tidw_.set_absent(true);
        tidw_.set_lock(true);
#ifdef CPR
        version_ = 0;
#endif
    }

    Record(const Record& right) {
        set_tidw(right.get_tidw());
        tuple_ = right.get_tuple();
        set_snap_ptr(right.get_snap_ptr());
    }

    Record(Record&& right) {
        set_tidw(right.get_tidw());
        tuple_ = std::move(right.get_tuple());
        set_snap_ptr(right.get_snap_ptr());
        right.set_snap_ptr(nullptr);
    }

    Record& operator=(const Record& right) { // NOLINT
        set_tidw(right.get_tidw());
        tuple_ = right.get_tuple();
        set_snap_ptr(right.get_snap_ptr());
        return *this;
    }

    Record& operator=(Record&& right) { // NOLINT
        set_tidw(right.get_tidw());
        tuple_ = std::move(right.get_tuple());
        set_snap_ptr(right.get_snap_ptr());
        right.set_snap_ptr(nullptr);
        return *this;
    }

    [[nodiscard]] Record* get_snap_ptr() const { return snap_ptr_.load(std::memory_order_acquire); } // NOLINT

    tid_word& get_tidw() { return tidw_; } // NOLINT

    [[nodiscard]] const tid_word& get_tidw() const { return tidw_; } // NOLINT

    Tuple& get_tuple() { return tuple_; } // NOLINT

    [[nodiscard]] const Tuple& get_tuple() const { return tuple_; } // NOLINT

    void set_tidw(tid_word tidw) & { tidw_.set_obj(tidw.get_obj()); }

    void set_snap_ptr(Record* ptr) { snap_ptr_.store(ptr, std::memory_order_release); }

#if defined(CPR)

    Tuple& get_stable() { return stable_; } // NOLINT

    [[nodiscard]] std::uint64_t get_version() const { return version_; } // NOLINT

    void set_version(std::uint64_t new_v) { version_ = new_v; }

#endif

private:
    s_mutex mtx_;
    /**
     * @brief Pointer to latest version
     * @details The version infomation which it should have at each version.
     */
    std::atomic<version*> latest_;
};

} // namespace shirakami
