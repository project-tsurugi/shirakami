#include <bitset>

#include "gtest/gtest.h"
#include "kvs/interface.h"

// shirakami-impl interface library
#include "tuple_local.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_search : public ::testing::Test {  // NOLINT
public:
    void SetUp() override { init(); }  // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(simple_search, search) {  // NOLINT
    std::string k("aaa");          // NOLINT
    std::string v("bbb");          // NOLINT
    Token s{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, k, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_search) {  // NOLINT
    std::string k("aaa");                 // NOLINT
    std::string v("bbb");                 // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_local_upsert) {  // NOLINT
    std::string k("aaa");                       // NOLINT
    std::string v("bbb");                       // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    Tuple* tuple{};
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_upsert_search) {  // NOLINT
    std::string k("aaa");                        // NOLINT
    std::string v("bbb");                        // NOLINT
    std::string v2("ccc");                       // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, upsert(s, k, v2));
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

// Begin tests to do multiple transactions concurrently by one thread.

TEST_F(simple_search, search_concurrent_insert) {  // NOLINT
    std::string k("k"); // NOLINT
    std::string v("k"); // NOLINT
    std::array<Token, 2> token_ar{};
    ASSERT_EQ(Status::OK, enter(token_ar.at(0)));
    ASSERT_EQ(Status::OK, enter(token_ar.at(1)));
    ASSERT_EQ(Status::OK, upsert(token_ar.at(0), k, v));
    Tuple* tuple{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT, search_key(token_ar.at(1), k, &tuple));
    ASSERT_EQ(Status::OK, commit(token_ar.at(0))); // NOLINT
    ASSERT_EQ(Status::OK, commit(token_ar.at(1))); // NOLINT
    ASSERT_EQ(Status::OK, leave(token_ar.at(0)));
    ASSERT_EQ(Status::OK, leave(token_ar.at(1)));
}

// End tests to do multiple transactions concurrently by one thread.

}  // namespace shirakami::testing
