/**
 * @file concurrency_control/include/tid.h
 * @brief utilities about transaction id
 */

#pragma once

#include <cstdint>

#include <ostream>
#include <type_traits>

#include "atomic_wrapper.h"
#include "epoch.h"

namespace shirakami {

class tid_word { // LINT
public:
    union { // LINT
        __uint128_t obj_;
        struct {
            bool lock_ : 1;
            bool lock_by_gc_ : 1;
            bool latest_ : 1;
            bool absent_ : 1;
            int : 8; // reserved
            std::uint64_t tid_ : 52;
            bool by_short_ : 1;
            int : 11; // reserved
            epoch::epoch_t epoch_ : 52;
        };
    };

    constexpr tid_word() // LINT
        : obj_(0) {
    } // LINT : clang-tidy order to initialize other member, but
    // it occurs compile error.
    constexpr tid_word(const __uint128_t obj) : obj_(obj) { } // LINT : the same as above.
    tid_word(const tid_word& right) noexcept     // LINT
        : obj_(right.get_obj()) {}               // LINT : the same as above.

    tid_word& operator=(const tid_word& right) { // LINT
        obj_ = right.get_obj();                  // LINT : union
        return *this;
    }

    bool operator==(const tid_word& right) const { // LINT : trailing
        return obj_ == right.get_obj();            // LINT : union
    }

    bool operator!=(const tid_word& right) const { // LINT : trailing
        return !operator==(right);
    }

    bool operator<(const tid_word& right) const { // LINT : trailing
        return this->obj_ < right.get_obj();      // LINT : union
    }

    bool operator>(const tid_word& right) const { // LINT : trailing
        return this->obj_ > right.get_obj();      // LINT : union
    }

    bool empty() { return obj_ == 0; } // LINT

    __uint128_t& get_obj() { return obj_; } // LINT

    const __uint128_t& get_obj() const { return obj_; } // LINT

    bool get_lock() { return lock_; } // LINT

    bool get_lock_by_gc() { return lock_by_gc_; } // LINT

    [[maybe_unused]] bool get_lock() const { return lock_; } // LINT

    [[maybe_unused]] [[nodiscard]] bool get_lock_by_gc() const {
        return lock_by_gc_; // LINT
    }

    [[maybe_unused]] bool get_latest() { return latest_; } // LINT

    [[maybe_unused]] bool get_latest() const { return latest_; } // LINT

    bool get_absent() { return absent_; } // LINT

    [[maybe_unused]] bool get_absent() const { return absent_; } // LINT

    uint64_t get_tid() { return tid_; } // LINT

    [[maybe_unused]] uint64_t get_tid() const { return tid_; } // LINT

    epoch::epoch_t get_epoch() { return epoch_; } // LINT

    [[maybe_unused]] [[nodiscard]] epoch::epoch_t get_epoch() const { // LINT
        return epoch_;                                                // LINT
    }

    [[maybe_unused]] bool get_by_short() const { return by_short_; } // LINT

    void inc_tid() { this->tid_ = this->tid_ + 1; } // LINT

    void reset() { obj_ = 0; } // LINT

    void set_absent(const bool absent) { this->absent_ = absent; } // LINT

    void set_epoch(epoch::epoch_t epo) {
        this->epoch_ = epo; // LINT
    }

    void set_latest(const bool latest) { this->latest_ = latest; } // LINT

    void set_lock(const bool lock) { this->lock_ = lock; } // LINT

    void set_lock_by_gc(const bool lock) { this->lock_by_gc_ = lock; } // LINT

    void set_by_short(const bool tf) { by_short_ = tf; } // LINT

    void set_obj(const __uint128_t obj) { this->obj_ = obj; } // LINT

    [[maybe_unused]] void set_tid(const uint64_t tid) {
        this->tid_ = tid; // LINT
    }

    void display();

    void lock(bool by_gc = false); // LINT

    /**
     * @pre This is called after lock() function.
     */
    void unlock() {
        tid_word new_tid = get_obj();
        new_tid.set_lock(false);
        new_tid.set_lock_by_gc(false);
        storeRelease(get_obj(), new_tid.get_obj());
    }

private:
};

static_assert(sizeof(tid_word) == sizeof(tid_word::obj_)); // LINT(*-union-access)
static_assert(std::is_nothrow_move_constructible_v<tid_word>);

inline std::ostream& operator<<(std::ostream& out, tid_word tid) { // LINT
    out << "lock_:" << tid.get_lock() << ", lock_by_gc:" << tid.get_lock_by_gc()
        << ", latest_:" << tid.get_latest() << ", absent_:" << tid.get_absent()
        << ", tid_:" << tid.get_tid() << ", by_short_:" << tid.get_by_short()
        << ", epoch_:" << tid.get_epoch();
    return out;
}

} // namespace shirakami
