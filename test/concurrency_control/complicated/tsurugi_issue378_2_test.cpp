
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

// NOLINTBEGIN

class tsurugi_issue378_2_test
    : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue378_2");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

void scan_and_read(Token& t, Storage &st, std::string_view lk, scan_endpoint le, std::string_view rk, scan_endpoint re) {
    ScanHandle sh;
    auto rc = open_scan(t, st, lk, le, rk, re, sh);
    if (rc == Status::OK) {
        while (next(t, sh) == Status::OK) { }
        ASSERT_OK(close_scan(t, sh));
    } else if (rc == Status::WARN_NOT_FOUND) {
        // nop
    } else {
        LOG(FATAL) << "open_scan rc:" << rc;
    }
}

void full_scan(Token& t, Storage &st) {
    scan_and_read(t, st, "", scan_endpoint::INF, "", scan_endpoint::INF);
}

INSTANTIATE_TEST_SUITE_P(
        revorder, tsurugi_issue378_2_test,
        ::testing::Values(false, true));

TEST_P(tsurugi_issue378_2_test, case_1) {
    bool rev = GetParam();
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;

    VLOG(30) << "TX1 begin";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(30) << "TX1 select full";
    full_scan(t1, st);
    VLOG(30) << "TX1 insert 2";
    ASSERT_OK(insert(t1, st, "2", "100"));
    wait_epoch_update();

    VLOG(30) << "TX2 begin";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(30) << "TX2 select full";
    full_scan(t2, st);
    VLOG(30) << "TX2 insert 3";
    ASSERT_OK(insert(t2, st, "3", "100"));
    wait_epoch_update();

    if (!rev) {  // commit TX1 -> TX2
        VLOG(30) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));
        VLOG(30) << "TX2 Commit (must fail)";
        ASSERT_EQ(commit(t2), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    } else {  // commit TX2 -> TX1
        std::atomic_bool t2c_done = false;
        std::atomic<Status> t2c_rc;

        VLOG(30) << "TX2 Commit (wait)";
        ASSERT_EQ(commit(t2, [&t2c_done, &t2c_rc](Status rc, [[maybe_unused]] reason_code reason, [[maybe_unused]] durability_marker_type dm){ t2c_rc = rc; t2c_done = true; }), false);

        VLOG(30) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        VLOG(30) << "TX2 ... resume Commit (fail)";
        while (!t2c_done) { _mm_pause(); }
        ASSERT_EQ(t2c_rc.load(), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    }
    ASSERT_OK(delete_storage(st));
}

TEST_P(tsurugi_issue378_2_test, case_2b) {
    bool rev = GetParam();
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;

    VLOG(30) << "TX1 begin";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(30) << "TX1 Select id=1";
    std::string val;
    ASSERT_EQ(search_key(t1, st, "1", val), Status::WARN_NOT_FOUND);
    VLOG(30) << "TX1 Insert 6";
    ASSERT_OK(insert(t1, st, "6", "600"));
    wait_epoch_update();

    VLOG(30) << "TX2 begin";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(30) << "TX2 Select full";
    full_scan(t2, st);
    VLOG(30) << "TX2 Insert 1";
    ASSERT_OK(insert(t2, st, "1", "100"));
    wait_epoch_update();

    if (!rev) {  // commit TX1 -> TX2
        VLOG(30) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        VLOG(30) << "TX2 Commit (must fail)";
        ASSERT_EQ(commit(t2), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    } else {  // commit TX2 -> TX1
        std::atomic_bool t2c_done = false;
        std::atomic<Status> t2c_rc;

        VLOG(30) << "TX2 Commit (wait)";
        ASSERT_EQ(commit(t2, [&t2c_done, &t2c_rc](Status rc, [[maybe_unused]] reason_code reason, [[maybe_unused]] durability_marker_type dm){ t2c_rc = rc; t2c_done = true; }), false);

        VLOG(30) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        VLOG(30) << "TX2 ... resume Commit (fail)";
        while (!t2c_done) { _mm_pause(); }
        ASSERT_EQ(t2c_rc.load(), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    }
    ASSERT_OK(delete_storage(st));
}

TEST_P(tsurugi_issue378_2_test, case_3b) {
    bool rev = GetParam();
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;

    // setup initial data
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(t1));
    ASSERT_OK(insert(t1, st, "1", "100"));
    ASSERT_OK(insert(t1, st, "2", "200"));
    ASSERT_OK(insert(t1, st, "3", "300"));
    ASSERT_OK(insert(t1, st, "4", "400"));
    ASSERT_OK(insert(t1, st, "5", "500"));
    ASSERT_OK(insert(t1, st, "6", "600"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));
    wait_epoch_update();

    VLOG(50) << "TX1 begin";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(30) << "TX1 Select 1";
    std::string val;
    ASSERT_OK(search_key(t1, st, "1", val));
    VLOG(30) << "TX1 Insert 7";
    ASSERT_OK(insert(t1, st, "7", "700"));
    wait_epoch_update();

    VLOG(30) << "TX2 begin";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(30) << "TX2 Select 2-INF";
    scan_and_read(t2, st, "2", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF);
    VLOG(30) << "TX2 Update 1";
    ASSERT_OK(update(t2, st, "1", "150"));
    wait_epoch_update();

    if (!rev) {  // commit TX1 -> TX2
        VLOG(30) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));
        wait_epoch_update();

        VLOG(30) << "TX2 Commit (must fail)";
        ASSERT_EQ(commit(t2), Status::ERR_CC);
        ASSERT_OK(leave(t2));
        wait_epoch_update();
    } else {  // commit TX2 -> TX1
        std::atomic_bool t2c_done = false;
        std::atomic<Status> t2c_rc;

        VLOG(30) << "TX2 Commit (wait)";
        ASSERT_EQ(commit(t2, [&t2c_done, &t2c_rc](Status rc, [[maybe_unused]] reason_code reason, [[maybe_unused]] durability_marker_type dm){ t2c_rc = rc; t2c_done = true; }), false);

        VLOG(30) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        VLOG(30) << "TX2 ... resume Commit (fail)";
        while (!t2c_done) { _mm_pause(); }
        ASSERT_EQ(t2c_rc.load(), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    }

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(t1));
    ASSERT_OK(search_key(t1, st, "1", val));
    ASSERT_EQ(val, "100");  // not updated
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(delete_storage(st));
}

// NOLINTEND

} // namespace shirakami::testing
