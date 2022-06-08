
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/version.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class termination_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-termination_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(termination_test, commit_long_long_low_high) { // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin(s1, TX_TYPE::LONG));
    ASSERT_EQ(Status::OK, tx_begin(s2, TX_TYPE::LONG));
    auto wait_next_epoch = []() {
        auto ep{epoch::get_global_epoch()};
        for (;;) {
            if (ep != epoch::get_global_epoch()) { break; }
            _mm_pause();
        }
    };
    wait_next_epoch();
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s1));                        // NOLINT
    ASSERT_EQ(Status::OK, commit(s2));                        // NOLINT
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(termination_test, commit_long_long_high_low) { // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin(s1, TX_TYPE::LONG));
    ASSERT_EQ(Status::OK, tx_begin(s2, TX_TYPE::LONG));
    auto wait_next_epoch = []() {
        auto ep{epoch::get_global_epoch()};
        for (;;) {
            if (ep != epoch::get_global_epoch()) { break; }
            _mm_pause();
        }
    };
    wait_next_epoch();
    ASSERT_EQ(Status::OK, commit(s1));                        // NOLINT
    ASSERT_EQ(Status::OK, commit(s2));                        // NOLINT
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
