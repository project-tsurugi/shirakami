
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_scan_long_upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-"
                "short_scan_long_upsert_test-scan_upsert_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/short_scan_long_upsert_test_test_log");
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
    static inline std::string log_dir_; // NOLINT
};

inline void wait_epoch_update() {
    epoch::epoch_t ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce == epoch::get_global_epoch()) {
            _mm_pause();
        } else {
            break;
        }
    }
}

TEST_F(short_scan_long_upsert_test, short_scan_find_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    ASSERT_EQ(tx_begin(ss), Status::OK);
    ASSERT_EQ(tx_begin(sb, false, true, {st}), Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(ss, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::ERR_FAIL_WP, read_key_from_scan(ss, hd, buf));
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(short_scan_long_upsert_test,
       short_scan_finish_before_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(tx_begin(ss), Status::OK);
        ASSERT_EQ(tx_begin(sb, false, true, {st}), Status::OK);
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(ss, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string buf{};
        ASSERT_EQ(Status::OK, read_key_from_scan(ss, hd, buf));
        ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    }
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(short_scan_long_upsert_test,
       short_scan_finish_after_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    // end prepare data
    // start test
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(tx_begin(ss), Status::OK);
        ASSERT_EQ(tx_begin(sb, false, true, {st}), Status::OK);
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(ss, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string buf{};
        ASSERT_EQ(Status::OK, read_key_from_scan(ss, hd, buf));
    }
    wait_epoch_update();
    ASSERT_EQ(Status::ERR_CONFLICT_ON_WRITE_PRESERVE, commit(ss)); // NOLINT
    // due to wp
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    // end test
    // cleanup test program
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

#if 0
TEST_F(short_scan_long_upsert_test, avoid_premature_by_wait) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};  // short
    Token s2{}; // long
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s2, false, true, {st}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s2, st, "", vb), Status::OK);
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(short_scan_long_upsert_test, reading_higher_priority_wp) { // NOLINT
    // prepare data and test search on higher priority WP (causing WARN_PREMATURE)
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s0{}; // short
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s0), Status::OK);
    ASSERT_EQ(Status::OK, insert(s0, st, "a", "A"));
    ASSERT_EQ(Status::OK, insert(s0, st, "b", "B"));
    ASSERT_EQ(Status::OK, commit(s0));
    // end of data preparation

    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(tx_begin(s1, false, true, {st}), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s2, false, true, {}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s2, st, "a", vb), Status::ERR_FAIL_WP);
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(short_scan_long_upsert_test, reading_lower_priority_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
    }
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s1, false, true, {}), Status::OK);
    ASSERT_EQ(tx_begin(s2, false, true, {st}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s1, st, "", vb), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

#endif

} // namespace shirakami::testing
