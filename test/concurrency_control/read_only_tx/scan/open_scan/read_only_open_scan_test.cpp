
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_open_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only_tx-"
                "scan-open_scan-read_only_open_scan_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(read_only_open_scan_test,             // NOLINT
       operation_before_after_start_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    ScanHandle hd{};
    stop_epoch();
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::READ_ONLY}),
              Status::OK);
    // operation before start epoch
    ASSERT_EQ(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd),
              Status::WARN_PREMATURE);
    resume_epoch();
    wait_epoch_update();
    // operation after start epoch
    ASSERT_NE(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd),
              Status::WARN_PREMATURE);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
