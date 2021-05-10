#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class simple_insert : public ::testing::Test { // NOLINT
public:
    void SetUp() override { init(); } // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(simple_insert, insert) { // NOLINT
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    {
        Tuple* tuple{};
        char k2 = 0;
        ASSERT_EQ(Status::OK, insert(s, storage, {&k2, 1}, v));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, search_key(s, storage, {&k2, 1}, &tuple));
        ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
        ASSERT_EQ(Status::OK, commit(s));
    }
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, insert(s, storage, "", v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, "", &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_insert, long_value_insert) { // NOLINT
    register_storage(storage);
    std::string k("CUSTOMER"); // NOLINT
    std::string v(             // NOLINT
            "b234567890123456789012345678901234567890123456789012345678901234567890"
            "12"
            "3456789012345678901234567890123456789012345678901234567890123456789012"
            "34"
            "5678901234567890123456789012345678901234567890123456789012345678901234"
            "56"
            "7890123456789012345678901234567890123456789012345678901234567890123456"
            "78"
            "9012345678901234567890123456789012345678901234567890123456789012345678"
            "90"
            "1234567890123456789012345678901234567890123456789012345678901234567890"
            "12"
            "3456789012345678901234567890123456789012345678901234567890123456789012"
            "34"
            "5678901234567890123456789012345678901234567890123456789012345678901234"
            "56"
            "7890123456789012345678901234567890123456789012345678901234567890123456"
            "78"
            "9012345678901234567890123456789012345678901234567890123456789012345678"
            "90"
            "1234567890123456789012345678901234567890123456789012345678901234567890"
            "12"
            "3456789012345678901234567890123456789012345678901234567890123456789012"
            "34"
            "5678901234567890123456789012345678901234567890");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_insert, long_key_insert) { // NOLINT
    register_storage(storage);
    std::string k(56, '0'); // NOLINT
    k += "a";
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple;
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}
} // namespace shirakami::testing
