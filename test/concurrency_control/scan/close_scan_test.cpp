#include <bitset>

#include "tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/close_scan_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, close_scan) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, handle));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
