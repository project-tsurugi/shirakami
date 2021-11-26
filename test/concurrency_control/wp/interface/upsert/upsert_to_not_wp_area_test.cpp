
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

class upsert_to_not_wp_area_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-upsert_to_not_wp_area_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/upsert_to_not_wp_area_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(upsert_to_not_wp_area_test, simple) { // NOLINT
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
    for (;;) {
        auto rc{upsert(s, st_1, k, v)};
        if (rc == Status::WARN_PREMATURE) {
            _mm_pause();
        } else if (rc == Status::WARN_INVALID_ARGS) {
            break;
        } else {
            LOG(FATAL) << rc;
        }
    }
    ASSERT_EQ(Status::OK, commit(s));

    // upsert with invalid wp
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st_1}));
    for (;;) {
        auto rc{upsert(s, st_2, k, v)};
        if (rc == Status::WARN_PREMATURE) {
            _mm_pause();
        } else if (rc == Status::WARN_INVALID_ARGS) {
            break;
        } else {
            LOG(FATAL) << rc;
        }
    }
    ASSERT_EQ(Status::OK, commit(s));

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
