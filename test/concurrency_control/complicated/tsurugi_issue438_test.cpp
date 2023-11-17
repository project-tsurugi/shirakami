
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

class tsurugi_issue438_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue438");
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

INSTANTIATE_TEST_SUITE_P(revorder, tsurugi_issue438_test,
                         ::testing::Values(false, true));

TEST_P(tsurugi_issue438_test, case_1) {
    bool rev = GetParam();
    Storage st{};
    ASSERT_OK(create_storage("test", st));
    wait_epoch_update();

    Token t1{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(insert(t1, st, "\x80\x00\x00\x01", "\x7f\xff\xff\x9b"));
    std::atomic<Status> t1c_rc{};
    ASSERT_TRUE(
            commit(t1, [&t1c_rc](Status rc, [[maybe_unused]] reason_code reason,
                                 [[maybe_unused]] durability_marker_type dm) {
                t1c_rc = rc;
            }));
    ASSERT_OK(t1c_rc);
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ScanHandle shd{};
    ASSERT_OK(open_scan(t1, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "\x80\x00\x00\x01");
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "\x7f\xff\xff\x9b");
    ASSERT_OK(delete_record(t1, st, "\x80\x00\x00\x01"));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t1, shd));
    ASSERT_OK(close_scan(t1, shd));
    ASSERT_TRUE(
            commit(t1, [&t1c_rc](Status rc, [[maybe_unused]] reason_code reason,
                                 [[maybe_unused]] durability_marker_type dm) {
                t1c_rc = rc;
            }));
    ASSERT_OK(t1c_rc);
    ASSERT_OK(leave(t1));

    // tx 1
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    // this must see deleted record hooking tree
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              search_key(t1, st, "\x80\x00\x00\x01", buf));
    ASSERT_OK(insert(t1, st, "\x80\x00\x00\x06", "\x7f\xff\xfd\xa7"));

    Token t2{};
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    if (Status::OK == open_scan(t2, st, "", scan_endpoint::INF, "",
                                scan_endpoint::INF, shd)) {
        ASSERT_EQ(Status::WARN_NOT_FOUND, read_key_from_scan(t2, shd, buf));
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd));
        ASSERT_OK(close_scan(t2, shd));
    }
    ASSERT_OK(insert(t2, st, "\x80\x00\x00\x01", "\x7f\xff\xff"));

    std::atomic<Status> t2c_rc{};
    std::atomic<bool> t2_done{false};
    if (!rev) { // commit TX1 -> TX2
        ASSERT_TRUE(commit(
                t1, [&t1c_rc](Status rc, [[maybe_unused]] reason_code reason,
                              [[maybe_unused]] durability_marker_type dm) {
                    t1c_rc = rc;
                }));
        ASSERT_EQ(t1c_rc, Status::OK);
        ASSERT_TRUE(commit(
                t2, [&t2c_rc](Status rc, [[maybe_unused]] reason_code reason,
                              [[maybe_unused]] durability_marker_type dm) {
                    t2c_rc = rc;
                }));
        ASSERT_EQ(t2c_rc, Status::ERR_CC);
    } else { // commit TX2 -> TX1
        ASSERT_FALSE(commit(
                t2, [&t2c_rc,
                     &t2_done](Status rc, [[maybe_unused]] reason_code reason,
                               [[maybe_unused]] durability_marker_type dm) {
                    t2c_rc = rc;
                    t2_done = true;
                }));
        ASSERT_TRUE(commit(
                t1, [&t1c_rc](Status rc, [[maybe_unused]] reason_code reason,
                              [[maybe_unused]] durability_marker_type dm) {
                    t1c_rc = rc;
                }));
        ASSERT_EQ(t1c_rc, Status::OK);
        while (!t2_done) { _mm_pause(); }
        ASSERT_EQ(t2c_rc, Status::ERR_CC);
    }

    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing
