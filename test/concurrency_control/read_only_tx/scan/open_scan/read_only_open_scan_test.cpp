
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

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
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(read_only_open_scan_test,  // NOLINT
       avoid_premature_by_wait) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(tx_begin(s, TX_TYPE::READ_ONLY), Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_NE(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd),
              Status::WARN_PREMATURE);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
