
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_scan_upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "scan_upsert-long_scan_upsert_test");
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

TEST_F(long_scan_upsert_test, reading_higher_priority_wp) { // NOLINT
    /**
     * prepare data and test search on higher priority WP
     * (causing WARN_PREMATURE)
     */
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s0{}; // short
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s0), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s0, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s0, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s0));
    wait_epoch_update();
    // end of data preparation

    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {}}),
              Status::OK);
    wait_epoch_update();
    session* ti1{static_cast<session*>(s1)};
    session* ti2{static_cast<session*>(s2)};
    ASSERT_NE(ti1->get_valid_epoch(), ti2->get_valid_epoch());
    std::string vb{};
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_NE(ti1->get_valid_epoch(), ti2->get_valid_epoch());
    ASSERT_EQ(
            *std::get<0>(ti2->get_overtaken_ltx_set().begin()->second).begin(),
            ti1->get_long_tx_id());
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_upsert_test, reading_lower_priority_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
    }
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {}}),
              Status::OK);
    wait_epoch_update();
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
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
    ASSERT_EQ(create_storage("", st), Status::OK);
    std::string init_val{"i"};
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
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
        ASSERT_EQ(tx_begin({s1,
                            transaction_options::transaction_type::LONG,
                            {st}}),
                  Status::OK);
        ASSERT_EQ(tx_begin({s2,
                            transaction_options::transaction_type::LONG,
                            {st}}),
                  Status::OK);
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
        ASSERT_EQ(Status::OK, read_value_from_scan(s2, hd2, vb)); // forwarding
        ASSERT_EQ(vb, init_val);
        ASSERT_EQ(upsert(s2, st, "", s2_val), Status::OK);
        ASSERT_EQ(Status::OK, commit(s1));
        auto* ti2{static_cast<session*>(s2)};
        ASSERT_EQ(1, ti2->get_overtaken_ltx_set().size());
        ASSERT_EQ(Status::ERR_CC, commit(s2));
        ASSERT_EQ(leave(s1), Status::OK);
        ASSERT_EQ(leave(s2), Status::OK);
    }
    {
        // test verify
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
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
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, buf));
    EXPECT_EQ(buf, "");
    ASSERT_EQ(Status::OK, commit(s));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_scan_upsert_test, scan_read_own_insert_on_absent) { // NOLINT
    // test for check_not_found in open_scan/next (with local write set)

    // prepare
    // storage: { "1": absent, "2": alive, "3": absent }
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};

    // stop Record GC
    std::unique_lock<std::mutex> lk{garbage::get_mtx_cleaner()};

    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "3", ""));
    ASSERT_OK(commit(s));
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, "1"));
    ASSERT_EQ(Status::OK, delete_record(s, st, "3"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // test
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    ltx_begin_wait(s);

    ASSERT_OK(upsert(s, st, "1", ""));
    ASSERT_OK(upsert(s, st, "3", ""));
    ScanHandle hd{};
    ASSERT_OK(open_scan(s, st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(s, hd, buf)); // read local write set
    EXPECT_EQ(buf, "1");
    ASSERT_OK(next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, buf));
    EXPECT_EQ(buf, "2");
    ASSERT_OK(next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, buf)); // read local write set
    EXPECT_EQ(buf, "3");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_OK(commit(s));

    // cleanup
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
