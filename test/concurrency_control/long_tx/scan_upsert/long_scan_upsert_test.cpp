
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
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_scan_upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "long_scan_upsert_test-long_tx_only_long_"
                                  "scan_upsert_test_test");
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

TEST_F(long_scan_upsert_test, reading_higher_priority_wp) { // NOLINT
    /**
     * prepare data and test search on higher priority WP 
     * (causing WARN_PREMATURE)
     */
    Storage st{};
    ASSERT_EQ(create_storage(st), Status::OK);
    Token s0{}; // short
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s0), Status::OK);
    ASSERT_EQ(Status::OK, insert(s0, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s0));
    wait_epoch_update();
    // end of data preparation

    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s1, TX_TYPE::LONG, {st}), Status::OK);
    wait_epoch_update();
    ASSERT_EQ(tx_begin(s2, TX_TYPE::LONG, {}), Status::OK);
    wait_epoch_update();
    session* ti1{static_cast<session*>(s1)};
    session* ti2{static_cast<session*>(s2)};
    ASSERT_NE(ti1->get_valid_epoch(), ti2->get_valid_epoch());
    std::string vb{};
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(ti1->get_valid_epoch(), ti2->get_valid_epoch());
    ASSERT_EQ(*ti2->get_overtaken_ltx_set().begin()->second.begin(),
              ti1->get_long_tx_id());
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_upsert_test, reading_lower_priority_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage(st), Status::OK);
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
    ASSERT_EQ(tx_begin(s1, TX_TYPE::LONG, {}), Status::OK);
    wait_epoch_update();
    ASSERT_EQ(tx_begin(s2, TX_TYPE::LONG, {st}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, vb));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_upsert_test, read_modify_write) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage(st), Status::OK);
    std::string init_val{"i"};
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK, upsert(s, st, "", init_val));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
        wait_epoch_update(); // For readable if some tx do forewarding.
    }
    std::string s1_val{"s1"};
    std::string s2_val{"s2"};
    {
        // test
        Token s1{}; // long
        Token s2{}; // long
        ASSERT_EQ(enter(s1), Status::OK);
        ASSERT_EQ(enter(s2), Status::OK);
        ASSERT_EQ(tx_begin(s1, TX_TYPE::LONG, {st}), Status::OK);
        ASSERT_EQ(tx_begin(s2, TX_TYPE::LONG, {st}), Status::OK);
        wait_epoch_update();
        std::string vb{};
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        ASSERT_EQ(Status::OK, read_value_from_scan(s1, hd, vb));
        ASSERT_EQ(vb, init_val);
        ASSERT_EQ(upsert(s1, st, "", s1_val), Status::OK);
        ScanHandle hd2{};
        ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd2));
        ASSERT_EQ(Status::OK, read_value_from_scan(s2, hd, vb)); // forwarding
        ASSERT_EQ(vb, init_val);
        ASSERT_EQ(upsert(s2, st, "", s2_val), Status::OK);
        ASSERT_EQ(Status::OK, commit(s1));
        auto* ti2{static_cast<session*>(s2)};
        ASSERT_EQ(1, ti2->get_overtaken_ltx_set().size());
        ASSERT_EQ(Status::ERR_VALIDATION, commit(s2));
        ASSERT_EQ(leave(s1), Status::OK);
        ASSERT_EQ(leave(s2), Status::OK);
    }
    {
        // test verify
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        std::string vb{};
        ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
        ASSERT_EQ(vb, s1_val);
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
    }
}

TEST_F(long_scan_upsert_test, scan_read_own_upsert) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG, {st}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, buf));
    ASSERT_EQ(Status::OK, commit(s));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing