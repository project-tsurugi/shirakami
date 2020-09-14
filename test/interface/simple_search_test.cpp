#include <bitset>

#include "gtest/gtest.h"
#include "kvs/interface.h"

// shirakami-impl interface library
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

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
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_search) {  // NOLINT
    std::string k("aaa");                 // NOLINT
    std::string v("bbb");                 // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s));
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
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_search, search_upsert_search) {  // NOLINT
    std::string k("aaa");                        // NOLINT
    std::string v("bbb");                        // NOLINT
    std::string v2("ccc");                       // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, upsert(s, k, v2));
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
