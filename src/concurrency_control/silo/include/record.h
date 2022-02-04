/**
 * @file concurrency_control/silo/include/record.h
 * @brief header about record
 */

#pragma once

#include <shared_mutex>

#include "cpu.h"
#include "tid.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/scheme.h"
#include "shirakami/tuple.h"

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

    void get_key(std::string& out) { return tuple_.get_key(out); }

    [[nodiscard]] Record* get_snap_ptr() const {
        return snap_ptr_.load(std::memory_order_acquire);
    } // NOLINT

    tid_word& get_tidw() { return tidw_; } // NOLINT

    [[nodiscard]] const tid_word& get_tidw() const { return tidw_; } // NOLINT

    Tuple& get_tuple() { return tuple_; } // NOLINT

    [[nodiscard]] const Tuple& get_tuple() const { return tuple_; } // NOLINT

    void set_tidw(tid_word tidw) & { tidw_.set_obj(tidw.get_obj()); }

    void set_snap_ptr(Record* ptr) {
        snap_ptr_.store(ptr, std::memory_order_release);
    }

    void set_value(std::string_view const v) {
        std::lock_guard<std::shared_mutex> lk{mtx_value_};
        tuple_.get_pimpl()->set_value(v);
    }

#if defined(CPR)

    Tuple& get_stable() { return stable_; } // NOLINT

    [[nodiscard]] std::uint64_t get_version() const {
        return version_;
    } // NOLINT

    void set_version(std::uint64_t new_v) { version_ = new_v; }

#endif

private:
    tid_word tidw_;
#if defined(CPR)
    /**
     * @pre Only lock owner can read-write this filed.
     * @todo consider type of member and round-trip
     */
    std::atomic<std::uint64_t> version_{0};
    Tuple stable_;
#endif
    std::shared_mutex mtx_value_;
    Tuple tuple_;
    /**
     * @details This is safely snapshot for read only transaction.
     */
    std::atomic<Record*> snap_ptr_{nullptr};
};

} // namespace shirakami
