
#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "compiler.h"
#include "concurrency_control/silo/include/tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};
class simple_upsert : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/simple_upsert_test_log");
        init(false, log_dir); // NOLINT
        register_storage(st);
    }

    void TearDown() override {
        delete_storage(st);
        fin();
    }
};

TEST_F(simple_upsert, upsert) { // NOLINT
    std::string k("aaa");       // NOLINT
    std::string v("aaa");       // NOLINT
    std::string v2("bbb");      // NOLINT
    Token s{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s));
#ifdef CPR
    while (Status::OK != search_key(s, st, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, st, k, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()),
              0);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_upsert, double_upsert) { // NOLINT
    std::string k("aaa");              // NOLINT
    std::string v("bbb");              // NOLINT
    std::string v2("ccc");             // NOLINT
    std::string v3("ddd");             // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, st, k, v3));
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v3.data(), v3.size()), 0);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_upsert, upsert_after_insert) { // NOLINT
    std::string k("K");
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
