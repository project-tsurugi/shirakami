#pragma once

#include <xmmintrin.h>

#include "concurrency_control/wp/include/epoch.h"

namespace shirakami {

static inline void wait_epoch_update() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

} // namespace shirakami