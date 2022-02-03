#include <bitset>

#include "concurrency_control/silo/include/tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"
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

Storage storage;

TEST_F(delete_insert_10chars_key, delete_insert_with_10chars) { // NOLINT
    ASSERT_EQ(register_storage(storage), Status::OK);
    std::string k("testing_a0"); // NOLINT
    std::string v("bbb");        // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k,
                                   scan_endpoint::INCLUSIVE, records));
    EXPECT_EQ(1, records.size());
    for (auto&& t : records) {
        std::string key{};
        t->get_key(key);
        ASSERT_EQ(Status::OK, delete_record(s, storage, key));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
