
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class insert_to_not_wp_area_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "insert_to_not_wp_area_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/insert_to_not_wp_area_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(insert_to_not_wp_area_test, simple) { // NOLINT
    Storage st_1{};
    ASSERT_EQ(register_storage(st_1), Status::OK);
    Storage st_2{};
    ASSERT_EQ(register_storage(st_2), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"k"};
    std::string v{"v"};
    // upsert with no wp
    ASSERT_EQ(Status::OK, tx_begin(s, false, true));
    auto wait_change_epoch = []() {
        auto ce{epoch::get_global_epoch()};
        for (;;) {
            if (ce != epoch::get_global_epoch()) { break; }
            _mm_pause();
        }
    };
    wait_change_epoch();
    ASSERT_EQ(insert(s, st_1, k, v), Status::WARN_WRITE_WITHOUT_WP);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // upsert with invalid wp
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st_1}));
    wait_change_epoch();
    ASSERT_EQ(insert(s, st_2, k, v), Status::WARN_WRITE_WITHOUT_WP);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing