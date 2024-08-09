
#include <vector>

#include "test_tool.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #849: when read a tombstone in snapshot, get previous alive value

namespace shirakami::testing {

class tsurugi_issue849_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tsurugi_issues-tsurugi_issue849");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

TEST_F(tsurugi_issue849_test, tombstone) {
    //           : "a"  "b"
    //  --------------------
    //  epoch e1 : "1"
    //  epoch e2 : del  "2"
    //  epoch e3 : "3"  del

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    Token l1{};
    Token l2{};
    Token l3{};

    // prepare record
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(l1));
    ASSERT_OK(enter(l2));
    ASSERT_OK(enter(l3));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "a", "1"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "b", "2"));
    ASSERT_OK(delete_record(s, st, "a"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "a", "3"));
    ASSERT_OK(delete_record(s, st, "b"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l3, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    // check
    std::string key;
    std::string val;

    using k_v_s = std::vector<std::pair<std::string, std::string>>;
    k_v_s exp1 = {{"a", "1"}};
    k_v_s exp2 = {{"b", "2"}};
    k_v_s exp3 = {{"a", "3"}};

    // check by point read
    EXPECT_EQ(search_key(l1, st, "a", val), Status::OK);
    EXPECT_EQ(val, "1");
    auto rc_1b = search_key(l1, st, "b", val);
    EXPECT_EQ(rc_1b, Status::WARN_NOT_FOUND) << "val:" << val;

    EXPECT_EQ(search_key(l2, st, "b", val), Status::OK);
    EXPECT_EQ(val, "2");
    auto rc_2a = search_key(l2, st, "a", val);
    EXPECT_EQ(rc_2a, Status::WARN_NOT_FOUND) << "val:" << val;

    EXPECT_EQ(search_key(l3, st, "a", val), Status::OK);
    EXPECT_EQ(val, "3");
    auto rc_3b = search_key(l3, st, "b", val);
    EXPECT_EQ(rc_3b, Status::WARN_NOT_FOUND) << "val:" << val;

    auto check_scan_read = [&st](Token t, const k_v_s& exp, const char *msg){
        ScanHandle sh;
        k_v_s act{};
        auto rc = open_scan(t, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, sh);
        if (rc == Status::OK) {
            do {
                std::string k;
                std::string v;
                auto rck = read_key_from_scan(t, sh, k);
                if (rck == Status::WARN_CONCURRENT_INSERT) continue;
                if (rck == Status::WARN_NOT_FOUND) continue;
                ASSERT_OK(rck);
                auto rcv = read_value_from_scan(t, sh, v);
                if (rcv == Status::WARN_CONCURRENT_INSERT) continue;
                if (rcv == Status::WARN_NOT_FOUND) continue;
                ASSERT_OK(rcv);
                act.emplace_back(std::move(k), std::move(v));
            } while (next(t, sh) == Status::OK);
            ASSERT_OK(close_scan(t, sh));
        } else if (rc == Status::WARN_NOT_FOUND) {
            // nop
        } else {
            LOG(FATAL) << "open_scan rc:" << rc;
        }
        EXPECT_EQ(act, exp) << msg;
    };
    ASSERT_NO_FATAL_FAILURE(check_scan_read(l1, exp1, "l1"));
    ASSERT_NO_FATAL_FAILURE(check_scan_read(l2, exp2, "l2"));
    ASSERT_NO_FATAL_FAILURE(check_scan_read(l3, exp3, "l3"));

    ASSERT_OK(commit(l1));
    ASSERT_OK(commit(l2));
    ASSERT_OK(commit(l3));

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(l1));
    ASSERT_OK(leave(l2));
    ASSERT_OK(leave(l3));
}

TEST_F(tsurugi_issue849_test, old_normal) {
    //           : "a"
    //  ---------------
    //  epoch e1 : "1"
    //  epoch e2 : "2"
    //  epoch e3 : "3"

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    Token l1{};
    Token l2{};
    Token l3{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(l1));
    ASSERT_OK(enter(l2));
    ASSERT_OK(enter(l3));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "a", "1"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "a", "2"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "a", "3"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l3, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    std::string val;
    EXPECT_EQ(search_key(l1, st, "a", val), Status::OK);
    EXPECT_EQ(val, "1");

    EXPECT_EQ(search_key(l2, st, "a", val), Status::OK);
    EXPECT_EQ(val, "2");

    EXPECT_EQ(search_key(l3, st, "a", val), Status::OK);
    EXPECT_EQ(val, "3");

    ASSERT_OK(commit(l1));
    ASSERT_OK(commit(l2));
    ASSERT_OK(commit(l3));

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(l1));
    ASSERT_OK(leave(l2));
    ASSERT_OK(leave(l3));
}

TEST_F(tsurugi_issue849_test, latest_deleted) {
    //           : "a"
    //  ---------------
    //  epoch e1 : "1"
    //  epoch e2 : del

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    Token l1{};
    Token l2{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(l1));
    ASSERT_OK(enter(l2));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "a", "1"));
    ASSERT_OK(commit(s));
    wait_epoch_update();
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(delete_record(s, st, "a"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // keep LTX for snapshot
    ASSERT_OK(tx_begin({l2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();

    // check
    std::string val;
    EXPECT_EQ(search_key(l1, st, "a", val), Status::OK);
    EXPECT_EQ(val, "1");

    auto rc_2 = search_key(l2, st, "b", val);
    EXPECT_EQ(rc_2, Status::WARN_NOT_FOUND) << "val:" << val;

    ASSERT_OK(commit(l1));
    ASSERT_OK(commit(l2));

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(l1));
    ASSERT_OK(leave(l2));
}

} // namespace shirakami::testing
