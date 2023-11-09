
#include <atomic>
#include <functional>

#include "shirakami/interface.h"

#include "concurrency_control/include/session.h"
#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue378 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue378");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue378, normal_order) { // NOLINT
    Storage st{};
    ASSERT_OK(create_storage("test", st));

    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "1", "\x7f\xff\xff\xfe"));

    // commit callback
    Status rs_for_cb1;
    reason_code rc_for_cb1;
    durability_marker_type dm_for_cb1;
    auto cb1 = [&rs_for_cb1, &rc_for_cb1, &dm_for_cb1](
                       Status rs, reason_code rc, durability_marker_type dm) {
        rs_for_cb1 = rs;
        rc_for_cb1 = rc;
        dm_for_cb1 = dm;
    };

    ASSERT_TRUE(commit(s, cb1));
    ASSERT_EQ(Status::OK, rs_for_cb1);

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle shd{};
    ASSERT_OK(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(s, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_OK(read_value_from_scan(s, shd, buf));
    ASSERT_EQ(buf, "\x7f\xff\xff\xfe");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, shd));
    ASSERT_OK(close_scan(s, shd));
    ASSERT_OK(insert(s, st, "2", "\x7f\xff\xff\xfd"));

    Token s2{};
    ASSERT_OK(enter(s2));
    ASSERT_OK(
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_OK(open_scan(s2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    ASSERT_OK(read_key_from_scan(s2, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_OK(read_value_from_scan(s2, shd, buf));
    ASSERT_EQ(buf, "\x7f\xff\xff\xfe");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s2, shd));
    ASSERT_OK(close_scan(s2, shd));
    ASSERT_OK(insert(s2, st, "3", "\x7f\xff\xff\xfd"));

    // commit callback
    Status rs_for_cb2;
    reason_code rc_for_cb2;
    durability_marker_type dm_for_cb2;
    std::atomic<bool> cb2_is_called{false};
    auto cb2 = [&rs_for_cb2, &rc_for_cb2, &dm_for_cb2, &cb2_is_called](
                       Status rs, reason_code rc, durability_marker_type dm) {
        rs_for_cb2 = rs;
        rc_for_cb2 = rc;
        dm_for_cb2 = dm;
        cb2_is_called.store(true, std::memory_order_release);
    };
    LOG(INFO);
    ASSERT_TRUE(commit(s, cb1));
    LOG(INFO);
    ASSERT_TRUE(commit(s2, cb2));
    LOG(INFO);

    ASSERT_EQ(rs_for_cb2, Status::OK);

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(s2));
}

TEST_F(tsurugi_issue378, rev) { // NOLINT
    Storage st{};
    ASSERT_OK(create_storage("test", st));

    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "1", "\x7f\xff\xff\xfe"));

    // commit callback
    Status rs_for_cb1;
    reason_code rc_for_cb1;
    durability_marker_type dm_for_cb1;
    auto cb1 = [&rs_for_cb1, &rc_for_cb1, &dm_for_cb1](
                       Status rs, reason_code rc, durability_marker_type dm) {
        rs_for_cb1 = rs;
        rc_for_cb1 = rc;
        dm_for_cb1 = dm;
    };

    ASSERT_TRUE(commit(s, cb1));
    ASSERT_EQ(Status::OK, rs_for_cb1);

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle shd{};
    ASSERT_OK(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(s, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_OK(read_value_from_scan(s, shd, buf));
    ASSERT_EQ(buf, "\x7f\xff\xff\xfe");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, shd));
    ASSERT_OK(close_scan(s, shd));
    ASSERT_OK(insert(s, st, "2", "\x7f\xff\xff\xfd"));

    Token s2{};
    ASSERT_OK(enter(s2));
    ASSERT_OK(
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_OK(open_scan(s2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    ASSERT_OK(read_key_from_scan(s2, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_OK(read_value_from_scan(s2, shd, buf));
    ASSERT_EQ(buf, "\x7f\xff\xff\xfe");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s2, shd));
    ASSERT_OK(close_scan(s2, shd));
    ASSERT_OK(insert(s2, st, "3", "\x7f\xff\xff\xfd"));

    // commit callback
    Status rs_for_cb2;
    reason_code rc_for_cb2;
    durability_marker_type dm_for_cb2;
    std::atomic<bool> cb2_is_called{false};
    auto cb2 = [&rs_for_cb2, &rc_for_cb2, &dm_for_cb2, &cb2_is_called](
                       Status rs, reason_code rc, durability_marker_type dm) {
        rs_for_cb2 = rs;
        rc_for_cb2 = rc;
        dm_for_cb2 = dm;
        cb2_is_called.store(true, std::memory_order_release);
    };

    ASSERT_FALSE(commit(s2, cb2));
    ASSERT_TRUE(commit(s, cb1));

    while (!cb2_is_called.load(std::memory_order_acquire)) { _mm_pause(); }

    auto* ti2 = static_cast<session*>(s2);
    LOG(INFO) << ti2->get_result_info().get_key();
    LOG(INFO) << ti2->get_result_info().get_storage_name();
    ASSERT_EQ(rs_for_cb2, Status::ERR_CC);
    ASSERT_EQ(rc_for_cb2, reason_code::CC_LTX_PHANTOM_AVOIDANCE);

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
