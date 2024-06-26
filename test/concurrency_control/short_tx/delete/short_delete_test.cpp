
#include <bitset>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class short_delete_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "delete-short_delete_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(short_delete_test, delete_) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(yakushima::status::OK, put<Record>(st, k, ""));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_delete_test, delete_at_non_existing_storage) { // NOLINT
    Storage st{};
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_STORAGE_NOT_FOUND, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_delete_test, read_only_mode_delete_) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_delete_test, short_delete_find_wp) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    Token s2{};
    // prepare record
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    // prepare wp
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    // enable wp
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_CONFLICT_ON_WRITE_PRESERVE,
              delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, abort(s));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_delete_test, long_key) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string long_key(1024 * 36, 'a'); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_INVALID_KEY_LENGTH, delete_record(s, st, long_key));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
