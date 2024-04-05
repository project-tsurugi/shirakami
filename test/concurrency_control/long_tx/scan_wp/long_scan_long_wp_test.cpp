
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/include/epoch.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_scan_long_wp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "long_scan_long_wp_test-scan_upsert_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(long_scan_long_wp_test, reading_higher_priority_wp) { // NOLINT
    // prepare data and test search on higher priority WP (causing WARN_PREMATURE)
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s0{}; // short
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s0), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s0, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s0, st, "a", "A"));
    ASSERT_EQ(Status::OK, insert(s0, st, "b", "B"));
    ASSERT_EQ(Status::OK, commit(s0));
    wait_epoch_update();
    // end of data preparation

    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {}}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(open_scan(s2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd),
              Status::OK);
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, buf));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_long_wp_test, reading_lower_priority_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
    }
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing
