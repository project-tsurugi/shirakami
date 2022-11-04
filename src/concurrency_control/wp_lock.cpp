
#include <xmmintrin.h>

#include "concurrency_control/include/wp_lock.h"

namespace shirakami::wp {

void wp_lock::lock() {
    std::uint64_t expected{obj.load(std::memory_order_acquire)};
    for (;;) {
        if (is_locked(expected)) {
            // locked by others
            _mm_pause();
            expected = obj.load(std::memory_order_acquire);
            continue;
        }
        std::uint64_t desired{expected | 1}; // NOLINT
        if (obj.compare_exchange_weak(expected, desired,
                                      std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
            break;
        }
    }
}

void wp_lock::unlock() {
    std::uint64_t desired{obj.load(std::memory_order_acquire)};
    std::uint64_t locked_num{desired >> 1}; // NOLINT
    ++locked_num;
    desired = locked_num << 1; // NOLINT
    obj.store(desired, std::memory_order_release);
}

} // namespace shirakami::wp