/**
 * @file concurrency_control/include/tid.h
 * @details implement about tid.
 */

#include <emmintrin.h>
#include <cstdint>
#include <bitset>
#include <iostream>

#include "include/tid.h"
#include "atomic_wrapper.h"

namespace shirakami {

void tid_word::lock(bool by_gc) { // LINT
    tid_word expected;
    tid_word desired;
    expected.get_obj() = loadAcquire(get_obj());
    for (;;) {
        if (expected.get_lock()) {
            _mm_pause();
            expected.get_obj() = loadAcquire(get_obj());
        } else {
            desired = expected;
            desired.set_lock(true);
            desired.set_lock_by_gc(by_gc);
            if (compareExchange(get_obj(), expected.get_obj(),
                                desired.get_obj())) {
                break;
            }
        }
    }
}

void tid_word::display() {
    std::cout << "obj_ : " << std::bitset<sizeof(obj_) * 8>(obj_) // NOLINT
              << std::endl;                                       // LINT
    std::cout << "lock_ : " << lock_ << std::endl;                // NOLINT
    std::cout << "latest_ : " << latest_ << std::endl;            // NOLINT
    std::cout << "absent_ : " << absent_ << std::endl;            // NOLINT
    std::cout << "tid_ : " << tid_ << std::endl;                  // NOLINT
    std::cout << "epoch_ : " << epoch_ << std::endl;              // NOLINT
}

} // namespace shirakami
