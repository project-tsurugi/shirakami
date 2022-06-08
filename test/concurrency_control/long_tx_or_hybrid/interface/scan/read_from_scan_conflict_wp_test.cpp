
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

class scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "scan_test");
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

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(scan_test, short_find_wp) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    Token s{}; // for short
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // prepare test
    Token sl{}; // for long
    ASSERT_EQ(Status::OK, enter(sl));
    ASSERT_EQ(Status::OK, tx_begin(sl, TX_TYPE::LONG, {st}));
    wait_change_epoch();

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // test
    std::string sb{};
    ASSERT_EQ(Status::ERR_FAIL_WP, read_key_from_scan(s, hd, sb));

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(sl));
}

TEST_F(scan_test, long_find_wp) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    Token s{}; // wp-er
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_change_epoch();

    // prepare test
    Token sl{}; // finder
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG, {st}));
    ASSERT_EQ(Status::OK, enter(sl));
    ASSERT_EQ(Status::OK, tx_begin(sl, TX_TYPE::LONG));
    wait_change_epoch();

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
