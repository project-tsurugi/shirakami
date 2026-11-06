#pragma once

#include <xmmintrin.h>

#include "concurrency_control/include/epoch.h"
#include "database/include/logging.h"

// shirakami/include/
#include "shirakami/interface.h"
#include "shirakami/transaction_state.h"

#include "gtest/gtest.h"

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)
#define EXPECT_OK(expr) EXPECT_EQ(expr, shirakami::Status::OK)

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

static inline database_options envflags() {
    database_options ret{};
    // check environ "SHIRAKAMI_TEST_ITERATOR_BASED_SCAN"
    constexpr bool iterator_based_scan_default = false;
    bool iterator_based_scan = iterator_based_scan_default;
    if (auto* envstr = std::getenv("SHIRAKAMI_TEST_ITERATOR_BASED_SCAN");
        envstr != nullptr && *envstr != '\0') {
        if (std::strcmp(envstr, "1") == 0) {
            iterator_based_scan = true;
        } else if (std::strcmp(envstr, "0") == 0) {
            iterator_based_scan = false;
        } else {
            LOG(INFO) << log_location_prefix << "invalid value is set for "
                      << "SHIRAKAMI_TEST_ITERATOR_BASED_SCAN; using default value";
        }
    }
    VLOG(log_debug) << log_location_prefix << "envflag: iterator_based_scan is "
                    << (iterator_based_scan ? "enabled" : "disabled")
                    << (iterator_based_scan == iterator_based_scan_default ? " (default)" : "");
    ret.set_iterator_based_scan(iterator_based_scan);
    return ret;
}

} // namespace shirakami
