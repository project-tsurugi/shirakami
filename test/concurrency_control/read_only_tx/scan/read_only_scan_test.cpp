
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
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only_tx-"
                "scan-read_only_scan_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_only_scan_test, start_no_long_tx_exist) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    stop_epoch();
    {
        ASSERT_EQ(Status::OK,
                  tx_begin({s,
                            transaction_options::transaction_type::READ_ONLY}));
        ScanHandle hd{};
        ASSERT_EQ(Status::WARN_PREMATURE,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
    }
    resume_epoch();
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_scan_test, start_before_epoch_long_tx_exist) { // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ScanHandle hd{};
    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::WARN_PREMATURE, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(read_only_scan_test, no_page_before_read_only_tx_begin) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
