
#include "clock.h"

#include "test_tool.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue751_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue751_test");
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

// related-to: tsurugi-issues issue #751
// related-to: shirakami issue #141

// an implementation for ti751/si141 causes this regression
TEST_F(tsurugi_issue751_test, jogasaki_update_range) {
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    std::string buf{};

    // prepare record
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    ASSERT_OK(upsert(s, st, "a", "s0"));
    ASSERT_OK(commit(s));
    // wait epoch change
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "b", "s1"));
    // full-scan
    ScanHandle shd{};
    ASSERT_OK(open_scan(s, st, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(s, shd, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_OK(next(s, shd));
    ASSERT_OK(read_key_from_scan(s, shd, buf));
    ASSERT_EQ(buf, "b");
    ASSERT_EQ(next(s, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(s, shd));
    // each it -> { delete it, upsert it }
    auto rc_del1 = delete_record(s, st, "a");
    ASSERT_TRUE(rc_del1 == Status::OK);
    ASSERT_OK(upsert(s, st, "a", "s2"));
    auto rc_del2 = delete_record(s, st, "b");
    ASSERT_TRUE(rc_del2 == Status::OK || rc_del2 == Status::WARN_CANCEL_PREVIOUS_UPSERT);
    ASSERT_OK(upsert(s, st, "b", "s3"));
    // commit
    ASSERT_OK(commit(s));

    // cleanup
    ASSERT_OK(leave(s));
}

// an implementation for ti751/si141 causes this regression
TEST_F(tsurugi_issue751_test, search_insert_delete) {
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    std::string buf{};

    // prepare record
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    ASSERT_OK(upsert(s, st, "a", "s0"));
    ASSERT_OK(commit(s));
    // wait epoch change
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    // read non-existent
    ASSERT_EQ(search_key(s, st, "b", buf), Status::WARN_NOT_FOUND);
    // upsert the same page
    ASSERT_OK(upsert(s, st, "b", "s1"));
    // delete the same page
    auto rc_del = delete_record(s, st, "b");
    ASSERT_TRUE(rc_del == Status::OK || rc_del == Status::WARN_CANCEL_PREVIOUS_UPSERT);
    // commit
    ASSERT_OK(commit(s));

    // cleanup
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
