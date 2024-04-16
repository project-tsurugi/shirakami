
#include <mutex>

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

class short_delete_insert_different_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_insert-short_delete_insert_different_tx_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

    static Storage& get_storage() { return storage_; }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline Storage storage_;            // NOLINT
};

TEST_F(short_delete_insert_different_tx_test, delete_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // data preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    // end preparation

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(delete_record(s2, st, ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s2));

    /**
     * s1 failed insert and depends on existing the records.
     * Internally, s1 executed tx read operation for the records.
     */
    ASSERT_EQ(Status::ERR_CC, commit(s1));

    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(short_delete_insert_different_tx_test, delete_insert_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    Token s3{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, enter(s3));

    // data preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    // end preparation

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(delete_record(s1, st, ""), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s2, st, "", ""));
    ASSERT_EQ(Status::OK,
              tx_begin({s3, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(delete_record(s3, st, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));

    /**
     * s1 failed insert and depends on existing the records.
     * Internally, s1 executed tx read operation for the records.
     */
    ASSERT_EQ(Status::ERR_CC, commit(s2));
    ASSERT_EQ(Status::ERR_KVS, commit(s3));

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s3));
}

} // namespace shirakami::testing
