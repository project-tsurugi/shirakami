
#include <bitset>

#ifdef WP

#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/record.h"

#endif

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami::testing {

using namespace shirakami;

class delete_insert_10chars_key : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/delete_insert_10chars_key_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

Storage st;

TEST_F(delete_insert_10chars_key, delete_insert_with_10chars) { // NOLINT
    ASSERT_EQ(register_storage(st), Status::OK);
    std::string k("testing_a0"); // NOLINT
    std::string v("bbb");        // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
    std::string key{};
    rec_ptr->get_key(key);
    ASSERT_EQ(Status::OK, delete_record(s, st, key));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
