
#include <bitset>

#include "concurrency_control/wp/include/tuple_local.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

class delete_upsert_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

Storage storage;

TEST_F(delete_upsert_test, delete_upsert) { // NOLINT
    create_storage(storage);
    std::string k("testing"); // NOLINT
    std::string v("bbb");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
