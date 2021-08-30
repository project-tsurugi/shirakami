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
        log_dir.append("/build/test_log/open_scan_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, open_scan_test) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ScanHandle handle2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    ASSERT_EQ(0, handle);
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle2));
    ASSERT_EQ(1, handle2);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, open_scan_test2) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1{"sa"};   // NOLINT
    std::string k2{"sa/"};  // NOLINT
    std::string k3{"sa/c"}; // NOLINT
    std::string k4{"sb"};   // NOLINT
    std::string v{"v"};     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k4, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, k2, scan_endpoint::INCLUSIVE, "sa0", scan_endpoint::EXCLUSIVE, handle));
    Tuple* tuple{};
#ifdef CPR
    while (Status::OK != read_from_scan(s, handle, &tuple)) {
        ;
    }
    while (Status::OK != read_from_scan(s, handle, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
