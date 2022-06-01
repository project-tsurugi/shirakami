
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        init(false, "/tmp/shirakami_c_read_from_scan_test"); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, read_from_scan) { // NOLINT
    Storage st{};
    register_storage(st);
    std::string k("aaa");  // NOLINT
    std::string k2("aab"); // NOLINT
    std::string k3("aac"); // NOLINT
    std::string k4("aad"); // NOLINT
    std::string k5("aae"); // NOLINT
    std::string k6("aa");  // NOLINT
    std::string v1("bbb"); // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, "", v1));
    ASSERT_EQ(Status::OK, insert(s, st, k, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k2, v2));
    ASSERT_EQ(Status::OK, insert(s, st, k3, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k5, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k6, v2));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    /**
     * test
     * if read_from_scan detects self write(update, insert), it read from owns.
     */
    std::string sb{};
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
              read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if it calls read_from_scan with invalid handle, it returns
     * Status::ERR_INVALID_HANDLE. if read_from_scan read all records in cache
     * taken at open_scan, it returns Status::WARN_SCAN_LIMIT.
     */
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_key_from_scan(s, 3, sb));
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k.data(), k.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v1.data(), v1.size()), 0);
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k3.data(), k3.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v1.data(), v1.size()), 0);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if read_from_scan detects the record deleted by myself, it function
     * returns Status::WARN_ALREADY_DELETE.
     */
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::WARN_ALREADY_DELETE, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, abort(s));

    /**
     * test
     * if read_from_scan detects the record deleted by others between open_scan
     * and read_from_scan, it function returns Status::WARN_CONCURRENT_DELETE which
     * means reading deleted record.
     */
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, delete_record(s2, st, k));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
