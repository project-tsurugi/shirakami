
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class update_to_not_wp_area_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "update_to_not_wp_area_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(update_to_not_wp_area_test, simple) { // NOLINT
    Storage st_1{};
    ASSERT_EQ(create_storage("1", st_1), Status::OK);
    Storage st_2{};
    ASSERT_EQ(create_storage("2", st_2), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"k"};
    std::string v{"v"};
    // prepare test data
    ASSERT_EQ(Status::OK, upsert(s, st_1, k, v));
    ASSERT_EQ(Status::OK, upsert(s, st_2, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // upsert with no wp
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG}));
    auto wait_change_epoch = []() {
        auto ce{epoch::get_global_epoch()};
        for (;;) {
            if (ce != epoch::get_global_epoch()) { break; }
            _mm_pause();
        }
    };
    wait_change_epoch();
    ASSERT_EQ(update(s, st_1, k, v), Status::WARN_WRITE_WITHOUT_WP);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // upsert with invalid wp
    ASSERT_EQ(
            Status::OK,
            tx_begin({s, transaction_options::transaction_type::LONG, {st_1}}));
    wait_change_epoch();
    ASSERT_EQ(update(s, st_2, k, v), Status::WARN_WRITE_WITHOUT_WP);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
