
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #1002??: the contents of safe snapshot may be modified by OCC

namespace shirakami::testing {

class tsurugi_issue1002_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue1002_test");
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

TEST_F(tsurugi_issue1002_test, scan_read) {
    // setup
    // storage = { A: 1 }

    // expect
    // RTX: begin                       -> OK
    // RTX: full scan                   -> OK, reads A
    // OCC: begin, insert B 2, commit   -> OK
    // RTX: full scan                   -> OK, reads A
    // RTX: commit                      -> OK

    // before ti#1002?? fix
    // RTX: begin                       -> OK
    // RTX: full scan                   -> OK, reads A
    // OCC: begin, insert B 2, commit   -> OK
    // RTX: full scan                   -> OK, reads A and B  <- wrong
    // RTX: commit                      -> OK

    Storage st{};
    ASSERT_OK(create_storage("", st));
    ScanHandle shd{};
    std::string buf{};
    Token s{};
    Token r{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(r));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "A", "1"));
    ASSERT_OK(commit(s));

    wait_epoch_update();

    ASSERT_OK(tx_begin({r, transaction_options::transaction_type::READ_ONLY}));
    ltx_begin_wait(r);

    // full-scan should read { A }
    ASSERT_OK(open_scan(r, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(r, shd, buf));
    ASSERT_EQ(buf, "A");
    ASSERT_EQ(next(r, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(r, shd));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "B", "2"));
    ASSERT_OK(commit(s));

    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();

    // full-scan should read { A }
    ASSERT_OK(open_scan(r, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(r, shd, buf));
    ASSERT_EQ(buf, "A");
    ASSERT_EQ(next(r, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(r, shd));
    ASSERT_OK(commit(r));

    ASSERT_OK(leave(s));
    ASSERT_OK(leave(r));
}

} // namespace shirakami::testing
