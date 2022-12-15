
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_read_from_scan_conflict_wp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-hybrid-scan_wp-"
                "short_read_from_scan_conflict_wp_test");
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

TEST_F(short_read_from_scan_conflict_wp_test, short_find_wp) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    Token s{}; // for short
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // prepare test
    Token sl{}; // for long
    ASSERT_EQ(Status::OK, enter(sl));
    ASSERT_EQ(
            Status::OK,
            tx_begin({sl, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // test
    std::string sb{};
    ASSERT_EQ(Status::ERR_FAIL_WP, read_key_from_scan(s, hd, sb));

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(sl));
}

} // namespace shirakami::testing
