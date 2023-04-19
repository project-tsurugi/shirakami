
#include <bitset>
#include <mutex>

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/tuple_local.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

class short_t1_delete_between_t2_delete_upsert_test
    : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(short_t1_delete_between_t2_delete_upsert_test, delete_upsert) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", "1"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    ASSERT_EQ(Status::OK, delete_record(s1, st, ""));
    ASSERT_EQ(Status::OK, delete_record(s2, st, ""));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, upsert(s1, st, "", "2"));
    // checked local delete and change it to update.
    ASSERT_EQ(Status::ERR_KVS, commit(s1)); // NOLINT

    // verify
    std::string buf{};
    ASSERT_NE(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing