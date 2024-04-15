/**
 * @file concurrency_control/include/tid.h
 * @brief utilities about transaction id
 */

#pragma once

#include <cstdint>
#include <xmmintrin.h>

#include "atomic_wrapper.h"
#include "epoch.h"

namespace shirakami {

class tid_word { // NOLINT
public:
    static const std::size_t bit_size_epoch = 31;

    union { // NOLINT
        uint64_t obj_;
        struct {
            bool lock_ : 1;
            bool lock_by_gc_ : 1;
            bool latest_ : 1;
            bool absent_ : 1;
            std::uint64_t tid_ : 28; // NOLINT
            bool by_short_ : 1;
            epoch::epoch_t epoch_ : bit_size_epoch; // NOLINT
        };
    };

    tid_word() // NOLINT
        : obj_(0) {
    } // NOLINT : clang-tidy order to initialize other member, but
    // it occurs compile error.
    tid_word(const uint64_t obj) { obj_ = obj; } // NOLINT : the same as above.
    tid_word(const tid_word& right)              // NOLINT
        : obj_(right.get_obj()) {}               // NOLINT : the same as above.

    tid_word& operator=(const tid_word& right) { // NOLINT
        obj_ = right.get_obj();                  // NOLINT : union
        return *this;
    }

    bool operator==(const tid_word& right) const { // NOLINT : trailing
        return obj_ == right.get_obj();            // NOLINT : union
    }

    bool operator!=(const tid_word& right) const { // NOLINT : trailing
        return !operator==(right);
    }

    bool operator<(const tid_word& right) const { // NOLINT : trailing
        return this->obj_ < right.get_obj();      // NOLINT : union
    }

    bool operator>(const tid_word& right) const { // NOLINT : trailing
        return this->obj_ > right.get_obj();      // NOLINT : union
    }

    bool empty() { return obj_ == 0; } // NOLINT

    uint64_t& get_obj() { return obj_; } // NOLINT

    const uint64_t& get_obj() const { return obj_; } // NOLINT

    bool get_lock() { return lock_; } // NOLINT

    bool get_lock_by_gc() { return lock_by_gc_; } // NOLINT

    [[maybe_unused]] bool get_lock() const { return lock_; } // NOLINT

    [[maybe_unused]] [[nodiscard]] bool get_lock_by_gc() const {
        return lock_by_gc_; // NOLINT
    }

    [[maybe_unused]] bool get_latest() { return latest_; } // NOLINT

    [[maybe_unused]] bool get_latest() const { return latest_; } // NOLINT

    bool get_absent() { return absent_; } // NOLINT

    [[maybe_unused]] bool get_absent() const { return absent_; } // NOLINT

    uint64_t get_tid() { return tid_; } // NOLINT

    [[maybe_unused]] uint64_t get_tid() const { return tid_; } // NOLINT

    epoch::epoch_t get_epoch() { return epoch_; } // NOLINT

    [[maybe_unused]] [[nodiscard]] epoch::epoch_t get_epoch() const { // NOLINT
        return epoch_;                                                // NOLINT
    }

    [[maybe_unused]] bool get_by_short() const { return by_short_; } // NOLINT

    void inc_tid() { this->tid_ = this->tid_ + 1; } // NOLINT

    void reset() { obj_ = 0; } // NOLINT

    void set_absent(const bool absent) { this->absent_ = absent; } // NOLINT

    void set_epoch(epoch::epoch_t epo) {
        this->epoch_ = epo; // NOLINT
    }

    void set_latest(const bool latest) { this->latest_ = latest; } // NOLINT

    void set_lock(const bool lock) { this->lock_ = lock; } // NOLINT

    void set_lock_by_gc(const bool lock) { this->lock_by_gc_ = lock; } // NOLINT

    void set_by_short(const bool tf) { by_short_ = tf; } // NOLINT

    void set_obj(const uint64_t obj) { this->obj_ = obj; } // NOLINT

    [[maybe_unused]] void set_tid(const uint64_t tid) {
        this->tid_ = tid; // NOLINT
    }

    void display();

    void lock(bool by_gc = false); // NOLINT

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

inline std::ostream& operator<<(std::ostream& out, tid_word tid) { // NOLINT
    out << "lock_:" << tid.get_lock() << ", lock_by_gc:" << tid.get_lock_by_gc()
        << ", latest_:" << tid.get_latest() << ", absent_:" << tid.get_absent()
        << ", tid_:" << tid.get_tid() << ", by_short_:" << tid.get_by_short()
        << ", epoch_:" << tid.get_epoch();
    return out;
}

} // namespace shirakami