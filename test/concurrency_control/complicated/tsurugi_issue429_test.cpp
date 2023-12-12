
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {
// NOLINTBEGIN

class tsurugi_issue429 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue429");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

// for issue#429 problem
// some cases of upsert fail unintentionally,
// and those failures leave incorrect inserting record,
// so later occ read may fail.


void scan_and_read(Token& t, Storage& st, bool read_key, bool read_value,
                   std::string_view lk, scan_endpoint le, std::string_view rk,
                   scan_endpoint re) {
    ScanHandle sh;
    auto rc = open_scan(t, st, lk, le, rk, re, sh);
    if (rc == Status::OK) {
        do {
            std::string str;
            if (read_key) { ASSERT_OK(read_key_from_scan(t, sh, str)); }
            if (read_value) { ASSERT_OK(read_value_from_scan(t, sh, str)); }
        } while (next(t, sh) == Status::OK);
        ASSERT_OK(close_scan(t, sh));
    } else if (rc == Status::WARN_NOT_FOUND) {
        // nop
    } else {
        LOG(FATAL) << "open_scan rc:" << rc;
    }
}

void full_scan_and_read(Token& t, Storage& st) {
    scan_and_read(t, st, true, true, "", scan_endpoint::INF, "",
                  scan_endpoint::INF);
}

// same API sequence as original scenario in issue429
TEST_F(tsurugi_issue429, case_1) {
    VLOG(10) << "case 1";
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;

    VLOG(10) << "TX1: begin ltx";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    LOG(INFO) << "t1 " << static_cast<session*>(t1)->get_long_tx_id();
    wait_epoch_update();
    VLOG(10) << "TX1: select full";
    full_scan_and_read(t1, st);
    VLOG(10) << "TX1: insert A";
    ASSERT_OK(insert(t1, st, "A", "A1"));
    wait_epoch_update();

    VLOG(10) << "TX2: begin ltx";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    LOG(INFO) << "t2 " << static_cast<session*>(t2)->get_long_tx_id();
    wait_epoch_update();
    VLOG(10) << "TX2: select B-C";
    scan_and_read(t2, st, true, true, "B", scan_endpoint::INCLUSIVE, "C",
                  scan_endpoint::INCLUSIVE);
    VLOG(10) << "TX2: upsert D";
    ASSERT_OK(upsert(t2, st, "D", "D2"));
    wait_epoch_update();
    std::atomic_bool t2c_done = false;
    std::atomic<Status> t2c_rc;
    VLOG(10) << "TX2: commit will waiting";
    ASSERT_EQ(commit(t2,
                     [&t2c_done,
                      &t2c_rc](Status rc, [[maybe_unused]] reason_code reason,
                               [[maybe_unused]] durability_marker_type dm) {
                         t2c_rc = rc;
                         t2c_done = true;
                     }),
              false);

    VLOG(10) << "TX1: commit will ok";
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    wait_epoch_update();

    VLOG(10) << "TX2: commit-wait returns ok";
    while (!t2c_done) { _mm_pause(); }
    ASSERT_EQ(t2c_rc.load(), Status::OK);
    ASSERT_OK(leave(t2));

    wait_epoch_update();

    Token t3;
    VLOG(10) << "TX3: begin occ";
    ASSERT_OK(enter(t3));
    ASSERT_OK(tx_begin(t3));
    wait_epoch_update();
    VLOG(10) << "TX3: select full";
    full_scan_and_read(t3, st);
    wait_epoch_update();
    VLOG(10) << "TX3: commit will ok";
    ASSERT_OK(commit(t3));
    ASSERT_OK(leave(t3));

    ASSERT_OK(delete_storage(st));
}

TEST_F(tsurugi_issue429, DISABLED_short_case) {
    VLOG(10) << "short case";
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t;

    VLOG(10) << "prepare THE table with data: A";
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin(t));
    ASSERT_OK(insert(t, st, "A", "A0"));
    ASSERT_OK(commit(t));
    ASSERT_OK(leave(t));
    wait_epoch_update();

    VLOG(10) << "TX: begin ltx";
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin({t, transaction_type::LONG, {st}}));
    wait_epoch_update();
    VLOG(10) << "TX: upsert D";
    ASSERT_OK(upsert(t, st, "D", "D2"));
    wait_epoch_update();
    VLOG(10) << "TX: abort";
    ASSERT_OK(abort(t));

    VLOG(10) << "TX: begin occ";
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin(t));
    VLOG(10) << "TX: select full";
    full_scan_and_read(t, st);
    VLOG(10) << "TX: commit will ok";
    ASSERT_OK(commit(t));
    ASSERT_OK(leave(t));

    ASSERT_OK(delete_storage(st));
}

// NOLINTEND

} // namespace shirakami::testing