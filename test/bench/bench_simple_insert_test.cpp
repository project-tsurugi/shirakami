#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "tuple_local.h"

#include "shirakami/interface.h"

#include "bench/include/gen_key.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class simple_insert : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/bench_simple_insert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_insert, long_key_insert) { // NOLINT
    register_storage(storage);
    std::size_t key_length = 8; // NOLINT
    constexpr std::size_t key_num = 3;
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, make_key(key_length, key_num), v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
#ifdef CPR
    while (Status::OK != search_key(s, storage, make_key(key_length, key_num), &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, make_key(key_length, key_num), &tuple));
#endif
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    key_length = 64; // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, make_key(key_length, key_num), v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#ifdef CPR
    while (Status::OK != search_key(s, storage, make_key(key_length, key_num), &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, make_key(key_length, key_num), &tuple));
#endif
    std::string str_key = make_key(key_length, key_num);
#ifdef CPR
    while (Status::OK != search_key(s, storage, str_key, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, str_key, &tuple));
#endif
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s));
}
} // namespace shirakami::testing
