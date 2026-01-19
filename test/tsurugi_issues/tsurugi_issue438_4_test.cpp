
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class tsurugi_issue438_4_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue438_4");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

TEST_F(tsurugi_issue438_4_test, simple) {
    { GTEST_SKIP() << "LONG is not supported"; }
    // https://github.com/project-tsurugi/tsurugi-issues/issues/438#issuecomment-1835644560
    Storage current_batch;
    Storage receipts;
    ASSERT_OK(create_storage("current_batch", current_batch));
    ASSERT_OK(create_storage("receipts", receipts));

    Token t;
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
    ASSERT_OK(insert(t, current_batch, "1", "1"));
    ASSERT_OK(commit(t));

    // tx2
    ASSERT_OK(tx_begin({t, transaction_type::LONG, {receipts}}));
    wait_epoch_update();
    ScanHandle shd{};
    ASSERT_OK(open_scan(t, current_batch, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd));
    std::string buf;
    ASSERT_OK(read_key_from_scan(t, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_OK(read_value_from_scan(t, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t, shd));
    ASSERT_OK(close_scan(t, shd));

    // tx3
    Token t2;
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::SHORT}));
    ScanHandle shd2{};
    ASSERT_OK(open_scan(t2, current_batch, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd2));
    ASSERT_OK(read_key_from_scan(t2, shd2, buf));
    ASSERT_EQ(buf, "1");
    std::string buf2;
    ASSERT_OK(read_value_from_scan(t2, shd2, buf2));
    ASSERT_EQ(buf2, "1");
    std::string buf3;
    ASSERT_OK(search_key(t2, current_batch, buf, buf3));
    ASSERT_EQ(buf3, "1");
    ASSERT_OK(delete_record(t2, current_batch, buf));
    ASSERT_OK(upsert(t2, current_batch, buf, "2"));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd2));
    ASSERT_OK(close_scan(t2, shd2));
    ASSERT_OK(commit(t2));

    // tx1
    Token t3;
    ASSERT_OK(enter(t3));
    ASSERT_OK(tx_begin({t3, transaction_type::LONG}));
    wait_epoch_update();
    ScanHandle shd3{};
    ASSERT_OK(open_scan(t3, current_batch, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd3));
    ASSERT_OK(read_key_from_scan(t3, shd3, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_OK(read_value_from_scan(t3, shd3, buf));
    ASSERT_EQ(buf, "2"); // read tx3
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t3, shd3));
    ASSERT_OK(close_scan(t3, shd3));
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(t3, receipts, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd3));
    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_called{false};
    reason_code rc{};
    auto cb = [&cb_rc, &was_called,
               &rc](Status rs, [[maybe_unused]] reason_code rc1,
                    [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        was_called.store(true, std::memory_order_release);
        rc = rc1;
    };
    /**
     * expect waiting at once, but at becoming waiting, tx tries to waiting
     * bypass and bypass t1, and not wait
    */
    ASSERT_FALSE(commit(t3, cb));
    /**
     * This needs for t3 aborts but it didn't exist at
     * https://github.com/project-tsurugi/tsurugi-issues/issues/438#issuecomment-1835644560
    */
    ASSERT_OK(insert(t, receipts, "2", "200"));
    ASSERT_OK(commit(t)); // this needs to release waiting of t3
    // note: this fits tsurugi issue 5 ex 3 but not fit for trace log
    while (!was_called) { _mm_pause(); }
    ASSERT_EQ(Status::ERR_CC, cb_rc); // expect error due to rub violation
    ASSERT_EQ(reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION, rc);

    ASSERT_OK(leave(t));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
}

} // namespace shirakami::testing
