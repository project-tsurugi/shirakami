#pragma once

#include <xmmintrin.h>

#include "concurrency_control/include/epoch.h"

namespace shirakami {

static inline void wait_epoch_update() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

static inline void wait_cc_safe_ss_epoch_update() {
    auto ce{epoch::get_cc_safe_ss_epoch()};
    for (;;) {
        if (ce != epoch::get_cc_safe_ss_epoch()) { break; }
        _mm_pause();
    }
}

} // namespace shirakami