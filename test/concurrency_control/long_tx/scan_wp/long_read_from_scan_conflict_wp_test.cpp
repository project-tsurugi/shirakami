
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_read_from_scan_conflict_wp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-scan_wp-"
                "long_read_from_scan_conflict_wp_test");
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

TEST_F(long_read_from_scan_conflict_wp_test, long_find_wp) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    Token s{}; // wp-er
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_epoch_update();

    // prepare test
    Token sl{}; // finder
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG, {st}));
    ASSERT_EQ(Status::OK, enter(sl));
    ASSERT_EQ(Status::OK, tx_begin(sl, TX_TYPE::LONG));
    wait_epoch_update();

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // test
    std::string sb{};
    // forwarding
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(sl));
}

} // namespace shirakami::testing