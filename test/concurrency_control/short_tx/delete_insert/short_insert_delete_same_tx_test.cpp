
#include <mutex>

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"
#include "test_tool.h"

namespace shirakami::testing {

class short_insert_delete_same_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_insert-short_insert_delete_same_tx_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(short_insert_delete_same_tx_test, absent2absent_vs_insert) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string buf{};
    Token s1{};
    Token s2{};

    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s1, st, k, "v1"));
    ASSERT_OK(insert(s2, st, k, "v2"));
    ASSERT_OK(delete_record(s1, st, k));  // ABSENT_TO_ABSENT (TOMBSTONE)
    ASSERT_OK(commit(s2)); // "k" : "v2"
    auto rc1 = commit(s1); // KVS check fail
    ASSERT_TRUE(rc1 == Status::ERR_CC || rc1 == Status::ERR_KVS);

    // verify
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(search_key(s1, st, k, buf));
    ASSERT_EQ(buf, "v2");
    ASSERT_OK(commit(s1));

    ASSERT_OK(leave(s2));
    ASSERT_OK(leave(s1));
}

} // namespace shirakami::testing
