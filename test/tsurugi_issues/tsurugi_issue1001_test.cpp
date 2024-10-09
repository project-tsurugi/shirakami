
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #1001: ongoing RTX session prevents CC safe ss advance

namespace shirakami::testing {

class tsurugi_issue1001_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tsurugi_issues-tsurugi_issue1001_test");
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

TEST_F(tsurugi_issue1001_test, second_rtx_should_read_newer_ss) {
    // setup
    // storage = { A: 1 }

    // expect
    // RTX1: begin                       -> OK
    // OCC:  begin, insert B 2, commit   -> OK
    // wait
    // RTX2: begin, full scan, commit    -> OK, reads A and B
    // RTX1: commit                      -> OK

    // before ti#1001 fix
    // RTX1: begin                       -> OK
    // OCC:  begin, insert B 2, commit   -> OK
    // wait
    // RTX2: begin, full scan, commit    -> OK, reads only A  <- wrong
    // RTX1: commit                      -> OK

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    Token r1{};
    Token r2{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(r1));
    ASSERT_OK(enter(r2));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "A", "1"));
    ASSERT_OK(commit(s));

    wait_epoch_update();

    ASSERT_OK(tx_begin({r1, transaction_options::transaction_type::READ_ONLY}));
    ltx_begin_wait(r1);

    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "B", "2"));
    ASSERT_OK(commit(s));

    wait_epoch_update();
    wait_epoch_update();

    ASSERT_OK(tx_begin({r2, transaction_options::transaction_type::READ_ONLY}));
    ltx_begin_wait(r2);

    wait_epoch_update();

    // full-scan should read { A, B }
    std::string buf{};
    ScanHandle shd{};
    ASSERT_OK(open_scan(r2, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(r2, shd, buf));
    ASSERT_EQ(buf, "A");
    ASSERT_OK(next(r2, shd));
    ASSERT_OK(read_key_from_scan(r2, shd, buf));
    ASSERT_EQ(buf, "B");
    ASSERT_EQ(next(r2, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(r2, shd));
    ASSERT_OK(commit(r2));

    ASSERT_OK(commit(r1));

    ASSERT_OK(leave(s));
    ASSERT_OK(leave(r1));
    ASSERT_OK(leave(r2));
}

} // namespace shirakami::testing
