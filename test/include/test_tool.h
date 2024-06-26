#pragma once

#include <xmmintrin.h>

#include "concurrency_control/include/epoch.h"
#include "database/include/logging.h"

// shirakami/include/
#include "shirakami/interface.h"
#include "shirakami/transaction_state.h"

#include "gtest/gtest.h"

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami {

static inline void init_for_test() { set_is_debug_mode(true); }

// about epoch

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

// about ltx

static inline void ltx_begin_wait(Token t) {
    TxStateHandle thd{};
    ASSERT_OK(acquire_tx_state_handle(t, thd));
    TxState ts{};
    do {
        ASSERT_OK(check_tx_state(thd, ts));
        std::this_thread::yield();
    } while (ts.state_kind() == TxState::StateKind::WAITING_START);
    ASSERT_OK(release_tx_state_handle(thd));
}

} // namespace shirakami
