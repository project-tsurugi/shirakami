#pragma once

#include <xmmintrin.h>

#include "concurrency_control/include/epoch.h"

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

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

static inline void stop_epoch() {
    epoch::set_perm_to_proc(1);
    while (epoch::get_perm_to_proc() != 0) { _mm_pause(); }
}

static inline void resume_epoch() { epoch::set_perm_to_proc(-1); }

} // namespace shirakami