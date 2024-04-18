
#include <emmintrin.h>
#include <stdint.h>
#include <atomic>
#include <string_view>

#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"
#include "atomic_wrapper.h"
#include "concurrency_control/include/tid.h"

namespace shirakami {

Record::~Record() {
    auto* ver = get_latest();
    while (ver != nullptr) {
        auto* ver_tmp = ver->get_next();
        delete ver; // NOLINT
        ver = ver_tmp;
    }
}

Record::Record(std::string_view const key) : key_(key) {
    latest_.store(new version(), std::memory_order_release); // NOLINT
    tidw_.set_lock(false);
    tidw_.set_latest(true);
    tidw_.set_absent(true);
}

Record::Record(std::string_view const key, std::string_view const val)
    : key_(key) {
    latest_.store(new version(val), std::memory_order_release); // NOLINT
    tidw_.set_lock(true);
    tidw_.set_latest(true);
    tidw_.set_absent(true);
}

[[nodiscard]] tid_word Record::get_stable_tidw() {
    for (;;) {
        tid_word check{loadAcquire(tidw_.get_obj())};
        if (check.get_lock()) {
            _mm_pause();
        } else {
            return check;
        }
    }
}

} // namespace shirakami