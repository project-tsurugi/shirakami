
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_reverse_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only_tx-"
                "scan-read_only_reverse_scan_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_only_reverse_scan_test, simple) { // NOLINT
    // verify simple scenario using reverse scan
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // prepare data
    std::string k0{"k"};
    std::string v0{"v"};
    std::string k1{"k1"};
    std::string v1{"v1"};
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k0, v0));
    ASSERT_EQ(Status::OK, upsert(s, st, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, hd, 1, true));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, sb));
    ASSERT_EQ(sb, v1);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_reverse_scan_test, bad_parameters) { // NOLINT
    // currently reverse scan must be called with max_size == 1 and r_end == INF
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::ERR_FATAL, open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, hd, 0, true));
    ASSERT_EQ(Status::ERR_FATAL, open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE, hd, 1, true));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_reverse_scan_test, fail_to_fetch_max_size) { // NOLINT
    // when maximum rec is tombstone, max_size=1 fails to fetch it
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // prepare data
    std::string k0{"k"};
    std::string v0{"v"};
    std::string k1{"k1"};
    std::string v1{"v1"};
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k0, v0));
    ASSERT_EQ(Status::OK, upsert(s, st, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    {
        // stop gc so that k1 is left as tombstone and open_scan results in not-found
        std::unique_lock<std::mutex> lk{garbage::get_mtx_cleaner()};

        ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, delete_record(s, st, k1));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        {
            ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
            wait_epoch_update();
            ScanHandle hd{};
            ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, hd, 1, true));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        }
    }
    {
        // expecting waiting some epochs fix the problem
        wait_epoch_update();
        ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
        wait_epoch_update();
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, hd, 1, true));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k0);
        ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, sb));
        ASSERT_EQ(sb, v0);
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
