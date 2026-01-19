
#include <emmintrin.h>
#include <atomic>
#include <mutex>
#include <string>

#include "shirakami/interface.h"
#include "test_tool.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/result_info.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"
#include "shirakami/tx_state_notification.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class tsurugi_issue378_1 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue378_1");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue378_1, case_1) { // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ScanHandle hd1{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s1, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd1));
    ASSERT_EQ(Status::OK, insert(s1, st, "2", "100"));

    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle hd2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s2, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd2));
    ASSERT_EQ(Status::OK, insert(s2, st, "3", "100"));

    ASSERT_EQ(Status::OK, commit(s1));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(tsurugi_issue378_1, case_1_rev) { // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
                                         // commit ordre is reverse of case 1
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ScanHandle hd1{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s1, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd1));
    ASSERT_EQ(Status::OK, insert(s1, st, "2", "100"));

    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle hd2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s2, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd2));
    ASSERT_EQ(Status::OK, insert(s2, st, "3", "100"));

    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_called{false};
    auto cb = [&cb_rc,
               &was_called](Status rs, [[maybe_unused]] reason_code rc,
                            [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        was_called.store(true, std::memory_order_release);
    };

    ASSERT_FALSE(commit(s2, cb));      // NOLINT
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    while (!was_called.load(std::memory_order_acquire)) { _mm_pause(); }

    ASSERT_EQ(Status::ERR_CC, cb_rc.load(std::memory_order_acquire));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
