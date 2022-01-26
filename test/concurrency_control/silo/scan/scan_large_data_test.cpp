#include <bitset>

#include "concurrency_control/silo/include/tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class scan_large_data_test : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/scan_large_data_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(scan_large_data_test, simple_large_data) { // NOLINT
    constexpr static size_t NUM_QUERIES = 200000;
    constexpr static size_t NUM_RECORDS = 100;

    Storage storage{};
    register_storage(storage);
    std::string v("0123456789012345678901234567890");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    for(std::size_t i=0, n=NUM_RECORDS; i<n; ++i) {
        std::string k{std::to_string(i)};
        ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, tx_begin(s, true));
    std::size_t read_count=0;
    for(std::size_t i=0, n=NUM_QUERIES; i<n; ++i) {
        ScanHandle handle{};
        Tuple* tuple{};
        {
            ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
            while (Status::OK != read_from_scan(s, handle, tuple)) {
                ++read_count;
            }
        }
        ASSERT_EQ(Status::OK, close_scan(s, handle));
    }
    std::cout << "read count : " << read_count << std::endl;

    EXPECT_EQ(1, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
