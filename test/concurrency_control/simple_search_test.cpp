#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "tuple_local.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

Storage storage;

class simple_search : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/simple_search_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_search, search) { // NOLINT
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#ifdef CPR
    while (Status::OK != search_key(s, storage, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_search) { // NOLINT
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
#ifdef CPR
    while (Status::OK != search_key(s, storage, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
#ifdef CPR
    while (Status::OK != search_key(s, storage, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_local_upsert) { // NOLINT
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    Tuple* tuple{};
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, storage, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_upsert_search) { // NOLINT
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("bbb");  // NOLINT
    std::string v2("ccc"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
#ifdef CPR
    while (Status::OK != search_key(s, storage, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v2));
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, storage, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, multiple_storages) { // NOLINT
    Storage storage0;
    Storage storage1;
    ASSERT_EQ(Status::OK, register_storage(storage0));
    ASSERT_EQ(Status::OK, register_storage(storage1));
    std::string k("k"); // NOLINT
    std::string v0("v0"); // NOLINT
    std::string v1("v1"); // NOLINT
    Token token{};
    ASSERT_EQ(Status::OK, enter(token));
    ASSERT_EQ(Status::OK, upsert(token, storage0, k, v0));
    EXPECT_EQ(Status::OK, upsert(token, storage1, k, v0));
    ASSERT_EQ(Status::OK, commit(token)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, upsert(token, storage0, k, v1));
    EXPECT_EQ(Status::OK, search_key(token, storage1, k, &tuple));
    EXPECT_EQ(memcmp(tuple->get_value().data(), v0.data(), v0.size()), 0);
    ASSERT_EQ(Status::OK, commit(token)); // NOLINT
}

// Begin tests to do multiple transactions concurrently by one thread.

TEST_F(simple_search, search_concurrent_insert) { // NOLINT
    register_storage(storage);
    std::string k("k"); // NOLINT
    std::string v("k"); // NOLINT
    std::array<Token, 2> token_ar{};
    ASSERT_EQ(Status::OK, enter(token_ar.at(0)));
    ASSERT_EQ(Status::OK, enter(token_ar.at(1)));
    ASSERT_EQ(Status::OK, upsert(token_ar.at(0), storage, k, v));
    Tuple* tuple{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT, search_key(token_ar.at(1), storage, k, &tuple));
    ASSERT_EQ(Status::OK, commit(token_ar.at(0))); // NOLINT
    ASSERT_EQ(Status::OK, commit(token_ar.at(1))); // NOLINT
    ASSERT_EQ(Status::OK, leave(token_ar.at(0)));
    ASSERT_EQ(Status::OK, leave(token_ar.at(1)));
}

// End tests to do multiple transactions concurrently by one thread.

} // namespace shirakami::testing
