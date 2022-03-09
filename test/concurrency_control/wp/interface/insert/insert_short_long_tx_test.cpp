
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

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class insert_short_long_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-upsert_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/upsert_test_log");
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
    static inline std::string log_dir_;       // NOLINT
};

TEST_F(insert_short_long_tx_test, longs_insert_after_shorts_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(tx_begin(s1, false, true, {st}), Status::OK);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(tx_begin(s2, false, false, {}), Status::OK);

    auto wait_epoch_update = []() {
        epoch::epoch_t ce{epoch::get_global_epoch()};
        for (;;) {
            if (ce == epoch::get_global_epoch()) {
                _mm_pause();
            } else {
                break;
            }
        }
    };
    wait_epoch_update();

    ASSERT_EQ(insert(s2, st, k, v), Status::WARN_FAIL_FOR_WP);
    ASSERT_EQ(insert(s1, st, k, v), Status::OK);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(insert_short_long_tx_test, shorts_insert_after_longs_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(tx_begin(s1, false, true, {st}), Status::OK);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(tx_begin(s2, false, false, {}), Status::OK);

    auto wait_epoch_update = []() {
        epoch::epoch_t ce{epoch::get_global_epoch()};
        for (;;) {
            if (ce == epoch::get_global_epoch()) {
                _mm_pause();
            } else {
                break;
            }
        }
    };
    wait_epoch_update();

    ASSERT_EQ(insert(s1, st, k, v), Status::OK);
    ASSERT_EQ(insert(s2, st, k, v), Status::WARN_FAIL_FOR_WP);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

} // namespace shirakami::testing
