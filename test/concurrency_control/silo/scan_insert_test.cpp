#include <bitset>

#include "concurrency_control/include/tuple_local.h"

#include "gtest/gtest.h"

#if defined(RECOVERY)

#include "boost/filesystem.hpp"

#endif

#include "shirakami/interface.h"

using namespace shirakami;

namespace shirakami::testing {

class scan_insert_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/scan_insert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(scan_insert_test, insert_record_after_select_doesnt_find) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k0("k0"); // NOLINT
    std::string k1("k1"); // NOLINT
    std::string v("v");   // NOLINT
    Token s0{};
    ASSERT_EQ(Status::OK, enter(s0));
    ASSERT_EQ(Status::OK, insert(s0, storage, k0, v));

    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s1, storage, k0, scan_endpoint::INCLUSIVE,
                                    k1, scan_endpoint::INCLUSIVE, handle));
    std::string value{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT,
              read_value_from_scan(s1, handle, value));
    ASSERT_EQ(Status::OK, next(s1, handle));
    ASSERT_EQ(value, ""); // NOLINT
    ASSERT_EQ(Status::OK, close_scan(s1, handle));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, insert(s0, storage, k1, v));
    ASSERT_EQ(Status::OK, commit(s0)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s0));
}

} // namespace shirakami::testing
