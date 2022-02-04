#include <bitset>

#include "concurrency_control/include/tuple_local.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

class delete_insert_16chars_key : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/delete_insert_16chars_key_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

Storage st;

TEST_F(delete_insert_16chars_key, delete_insert_with_16chars) { // NOLINT
    ASSERT_EQ(register_storage(st), Status::OK);
    std::string k("testing_a0123456"); // NOLINT
    std::string v("bbb");              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle handle;
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k,
                                    scan_endpoint::INCLUSIVE, handle));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    std::string key{};
    tuple->get_key(key);
    ASSERT_EQ(Status::OK, delete_record(s, st, key));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
