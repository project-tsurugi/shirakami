#include <bitset>

#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class delete_insert_16chars_key : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

Storage st;

TEST_F(delete_insert_16chars_key, delete_insert_with_16chars) { // NOLINT
    ASSERT_EQ(create_storage(st), Status::OK);
    std::string k("testing_a0123456"); // NOLINT
    std::string v("bbb");              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
    std::string sb{};
    rec_ptr->get_key(sb);
    ASSERT_EQ(Status::OK, delete_record(s, st, sb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
