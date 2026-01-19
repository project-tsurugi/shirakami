
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

TEST_F(tsurugi_issue1001_test, record_gc_never_unhook_records_on_alive_ss) {
    { GTEST_SKIP() << "LONG is not supported"; }
    // setup
    // storage = { A: 1 }

    // expect
    // LTX:  begin wp={}                 -> OK
    // OCC:  begin, delete A, commit     -> OK
    // RTX1: begin                       -> OK, valid_epoch < begin_epoch
    // LTX:  commit                      -> OK
    // RTX1: full scan                   -> OK, reads A
    // RTX2: begin                       -> OK, valid_epoch = begin_epoch+1
    // RTX2: full scan                   -> reads none
    // wait
    // RecGC: min valid_epoch(=RTX1's) < deleted epoch < min begin_epoch(=RTX1's) < cc_safe_ss_epoch
    // RecGC:                            -> never unhook A
    // RTX2: commit                      -> OK
    // RTX1: full scan                   -> OK, reads A
    // RTX1: commit                      -> OK

    // if recgc threshold is wrong
    // LTX:  begin wp={}                 -> OK
    // OCC:  begin, delete A, commit     -> OK
    // RTX1: begin                       -> OK, valid_epoch < begin_epoch
    // LTX:  commit                      -> OK
    // RTX1: full scan                   -> OK, reads A
    // RTX2: begin                       -> OK, valid_epoch = begin_epoch+1
    // RTX2: full scan                   -> reads none
    // wait
    // RecGC: deleted epoch < min begin_epoch(=RTX1's) < selected valid_epoch(=RTX2's) < cc_safe_ss_epoch
    // RecGC:                            -> unhook A        <- wrong
    // RTX2: commit                      -> OK
    // RTX1: full scan                   -> OK, reads none  <- wrong
    // RTX1: commit                      -> OK              <- wrong (scan result changed)

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    Token l{};
    Token r1{};
    Token r2{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(l));
    ASSERT_OK(enter(r1));
    ASSERT_OK(enter(r2));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "A", "1"));
    ASSERT_OK(commit(s));

    wait_epoch_update();

    ASSERT_OK(tx_begin({l, transaction_options::transaction_type::LONG, {}}));
    ltx_begin_wait(l);

    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(delete_record(s, st, "A"));
    ASSERT_OK(commit(s));

    wait_epoch_update();

    ASSERT_OK(tx_begin({r1, transaction_options::transaction_type::READ_ONLY}));
    ltx_begin_wait(r1);

    ASSERT_OK(commit(l));

    std::string buf{};
    ScanHandle shd{};
    // RTX1 full-scan should read { A }
    ASSERT_OK(open_scan(r1, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(r1, shd, buf));
    ASSERT_EQ(buf, "A");
    ASSERT_EQ(next(r1, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(r1, shd));

    wait_epoch_update();

    ASSERT_OK(tx_begin({r2, transaction_options::transaction_type::READ_ONLY}));
    ltx_begin_wait(r2);

    // RTX2 full-scan should read { }
    auto rcs2 = open_scan(r2, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd);
    // {open_scan -> NOT_FOUND}  OR  {open_scan -> OK, read_from_scan -> NOT_FOUND, next -> SCAN_LIMIT}
    ASSERT_TRUE(rcs2 == Status::WARN_NOT_FOUND || rcs2 == Status::OK);
    if (rcs2 == Status::OK) {
        ASSERT_EQ(read_key_from_scan(r2, shd, buf), Status::WARN_NOT_FOUND);
        ASSERT_EQ(next(r2, shd), Status::WARN_SCAN_LIMIT);
        ASSERT_OK(close_scan(r2, shd));
    }

    // wait Record GC with both RTX1 and RTX2
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();

    ASSERT_OK(commit(r2));

    // RTX1 full-scan should read { A }
    ASSERT_OK(open_scan(r1, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(r1, shd, buf));
    ASSERT_EQ(buf, "A");
    ASSERT_EQ(next(r1, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(r1, shd));
    ASSERT_OK(commit(r1));

    ASSERT_OK(leave(s));
    ASSERT_OK(leave(l));
    ASSERT_OK(leave(r1));
    ASSERT_OK(leave(r2));
}

} // namespace shirakami::testing
