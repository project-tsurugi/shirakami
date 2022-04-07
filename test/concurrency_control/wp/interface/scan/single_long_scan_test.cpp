
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class single_long_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "single_long_scan_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/single_long_scan_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(single_long_scan_test, start_before_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
        ScanHandle hd{};
        ASSERT_EQ(Status::WARN_PREMATURE,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
    } // start epoch
    wait_change_epoch();
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_long_scan_test, no_page_before_long_tx_begin) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
    wait_change_epoch();
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_long_scan_test, write_one_page_before_long_tx_begin) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // prepare data
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
    wait_change_epoch();
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

TEST_F(single_long_scan_test,
       write_one_page_between_long_begin_and_long_start) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sl{}; // long
    ASSERT_EQ(Status::OK, enter(ss));
    ASSERT_EQ(Status::OK, enter(sl));
    // prepare data
    std::string k{"k"};
    std::string v{"v"};
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK, tx_begin(sl, false, true, {st}));
        ASSERT_EQ(Status::OK, upsert(ss, st, k, v));
        ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    }
    wait_change_epoch();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k);
    ASSERT_EQ(Status::OK, read_value_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, v);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(sl, hd));
    ASSERT_EQ(Status::OK, commit(sl));
    ASSERT_EQ(Status::OK, leave(ss));
    ASSERT_EQ(Status::OK, leave(sl));
}

TEST_F(single_long_scan_test,
       write_one_page_between_long_start_and_inf) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sl{}; // long
    ASSERT_EQ(Status::OK, enter(ss));
    ASSERT_EQ(Status::OK, enter(sl));
    // prepare data
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(Status::OK, tx_begin(sl, false, true, {st}));
    wait_change_epoch();
    ASSERT_EQ(Status::OK, upsert(ss, st, k, v));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, commit(sl));
    ASSERT_EQ(Status::OK, leave(ss));
    ASSERT_EQ(Status::OK, leave(sl));
}

} // namespace shirakami::testing
