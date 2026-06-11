
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #1453: split vs phantom check

namespace shirakami::testing {

class tsurugi_issue1453_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tsurugi_issues-tsurugi_issue1453_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

// TODO: move another location
TEST_F(tsurugi_issue1453_test, insert_into_select) {
    if (!get_scan_mode_iterator_based()) { GTEST_SKIP() << "this test fails for vscan"; }
    // regression test: split by single insert
    // setup
    // storage: 00, 01, 02, 03, 04, 05, 06, 07 | 08, 09, 0A, 0B, 0C, 0D, 0E, 0F

    // expect
    // OCC1: begin                       -> OK
    // OCC1: open_scan                   -> OK
    // OCC1: read_key_scan               -> OK 00
    // OCC1: insert 10                   -> OK
    // OCC1: next                        -> OK
    // OCC1: read_key_scan               -> OK 01
    // OCC1: insert 11                   -> OK
    // OCC1: next                        -> OK
    // ...
    // OCC1: read_key_scan               -> OK 07
    // OCC1: insert 17                   -> OK
    // OCC1: next                        -> OK
    // OCC1: read_key_scan               -> OK 08
    // OCC1: insert 18                   -> OK
    // OCC1: next                        -> OK
    // ...
    // OCC1: read_key_scan               -> OK 0F
    // OCC1: insert 1F                   -> OK
    // OCC1: next                        -> OK
    // OCC1: read_key_scan               -> WARN_CONCURRENT_INSERT (10)
    // ...
    // OCC1: next                        -> OK
    // OCC1: read_key_scan               -> WARN_CONCURRENT_INSERT (1F)
    // OCC1: next                        -> WARN_SCAN_LIMIT
    // OCC1: close_scan                  -> OK
    // OCC1: commit                      -> OK

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    ASSERT_OK(enter(s1));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (int i = 0; i < 16; i++) {
        char buf[3];
        sprintf(buf, "%02X", i);
        ASSERT_OK(insert(s1, st, std::string_view(buf, 2U), "0")) << i;
    }
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    ScanHandle sh;
    ASSERT_OK(open_scan(s1, st, "", scan_endpoint::INF, "", scan_endpoint::INF, sh));
    for (int i = 0; ; ) {
        std::string buf{};
        auto rc_read_key_from_scan = read_key_from_scan(s1, sh, buf);
        if (rc_read_key_from_scan == Status::OK) {
            buf.at(0) = '1'; // 0X -> 1X
            ASSERT_OK(insert(s1, st, buf, "1")) << i;
            i++;
        } else if (rc_read_key_from_scan == Status::WARN_CONCURRENT_INSERT) {
            // nop
        } else {
            FAIL() << rc_read_key_from_scan << " " << i;
        }
        auto rcnext = next(s1, sh);
        if (i < 16) {
            EXPECT_EQ(rcnext, Status::OK) << i;
        } else {
            // WARN_SCAN_LIMIT
            // or OK, but can't read from scan
            if (rcnext == Status::WARN_SCAN_LIMIT) {
                EXPECT_EQ(i, 16);
                break;
            }
            if (rcnext == Status::OK) {
                buf.clear();
                rc_read_key_from_scan = read_key_from_scan(s1, sh, buf);
                ASSERT_EQ(rc_read_key_from_scan, Status::WARN_CONCURRENT_INSERT) << rc_read_key_from_scan << " " << i;
            }
        }
    }
    ASSERT_OK(close_scan(s1, sh));
    ASSERT_OK(commit(s1));

    ASSERT_OK(leave(s1));
}

} // namespace shirakami::testing
