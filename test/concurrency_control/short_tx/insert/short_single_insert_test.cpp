
#include <bitset>
#include <mutex>
#include <thread>

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st;
class single_insert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert-single_insert_test");
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

TEST_F(single_insert, read_only_mode_insert) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, insert(s, st, "", ""));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_insert, insert_at_non_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_STORAGE_NOT_FOUND, insert(s, 0, "", ""));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_insert, long_value_insert) { // NOLINT
    create_storage("", st);
    std::string k("CUSTOMER"); // NOLINT
    std::string v(             // NOLINT
            "b23456789012345678901234567890123456789012345678901234567890123456"
            "7890"
            "12"
            "345678901234567890123456789012345678901234567890123456789012345678"
            "9012"
            "34"
            "567890123456789012345678901234567890123456789012345678901234567890"
            "1234"
            "56"
            "789012345678901234567890123456789012345678901234567890123456789012"
            "3456"
            "78"
            "901234567890123456789012345678901234567890123456789012345678901234"
            "5678"
            "90"
            "123456789012345678901234567890123456789012345678901234567890123456"
            "7890"
            "12"
            "345678901234567890123456789012345678901234567890123456789012345678"
            "9012"
            "34"
            "567890123456789012345678901234567890123456789012345678901234567890"
            "1234"
            "56"
            "789012345678901234567890123456789012345678901234567890123456789012"
            "3456"
            "78"
            "901234567890123456789012345678901234567890123456789012345678901234"
            "5678"
            "90"
            "123456789012345678901234567890123456789012345678901234567890123456"
            "7890"
            "12"
            "345678901234567890123456789012345678901234567890123456789012345678"
            "9012"
            "34"
            "5678901234567890123456789012345678901234567890");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_insert, long_key_insert) { // NOLINT
    create_storage("", st);
    std::string k(56, '0'); // NOLINT
    k += "a";
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
    std::string b{};
    rec_ptr->get_key(b);
    ASSERT_EQ(k, b);
    rec_ptr->get_value(b);
    ASSERT_EQ(v, b);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_insert, insert_user_abort) { // NOLINT
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "k", "v"));
    ASSERT_EQ(Status::OK, abort(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
