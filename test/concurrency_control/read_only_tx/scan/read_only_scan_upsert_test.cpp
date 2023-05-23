
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_scan_upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only_tx-"
                "scan-read_only_scan_upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_only_scan_upsert_test,                 // NOLINT
       write_one_page_before_read_only_tx_begin) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // prepare data
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, sb));
    ASSERT_EQ(sb, v);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_scan_upsert_test,                               // NOLINT
       write_one_page_between_read_only_begin_and_valid_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token ss{}; // short
    Token sl{}; // read_only
    ASSERT_EQ(Status::OK, enter(ss));
    ASSERT_EQ(Status::OK, enter(sl));
    // prepare data
    std::string k{"k"};
    std::string v{"v"};
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK,
                  tx_begin({sl,
                            transaction_options::transaction_type::READ_ONLY}));
        ASSERT_EQ(Status::OK,
                  tx_begin({ss, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(ss, st, k, v));
        ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    }
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k);
    ASSERT_EQ(Status::OK, read_value_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, v);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(sl, hd));
    ASSERT_EQ(Status::OK, leave(ss));
    ASSERT_EQ(Status::OK, leave(sl));
}

TEST_F(read_only_scan_upsert_test,         // NOLINT
       write_one_page_after_valid_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token ss{}; // short
    Token sl{}; // read_only
    ASSERT_EQ(Status::OK, enter(ss));
    ASSERT_EQ(Status::OK, enter(sl));
    // prepare data
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(Status::OK,
              tx_begin({sl, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({ss, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(ss, st, k, v));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(sl, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, leave(ss));
    ASSERT_EQ(Status::OK, leave(sl));
}

} // namespace shirakami::testing