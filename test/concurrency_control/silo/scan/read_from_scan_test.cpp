
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/test_log/read_from_scan_test_log");
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
    static inline std::string log_dir_; // NOLINT
};

TEST_F(simple_scan, read_from_scan) { // NOLINT
    Storage storage{};
    register_storage(storage);
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
    ASSERT_EQ(Status::OK, insert(s, storage, "", v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v2));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k5, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k6, v2));
    ScanHandle handle{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    /**
     * test
     * if read_from_scan detects self write(update, insert), it read from owns.
     */
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
              read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if it calls read_from_scan with invalid handle, it returns
     * Status::ERR_INVALID_HANDLE. if read_from_scan read all records in cache
     * taken at open_scan, it returns Status::WARN_SCAN_LIMIT.
     */
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_from_scan(s, 3, tuple));
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    EXPECT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    std::string key{};
    tuple->get_key(key);
    EXPECT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
    std::string val{};
    tuple->get_value(val);
    EXPECT_EQ(memcmp(val.data(), v1.data(), v1.size()), 0);
    EXPECT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    tuple->get_key(key);
    EXPECT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
    tuple->get_value(val);
    EXPECT_EQ(memcmp(val.data(), v2.data(), v2.size()), 0);
    EXPECT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    tuple->get_key(key);
    EXPECT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
    tuple->get_value(val);
    EXPECT_EQ(memcmp(val.data(), v1.data(), v1.size()), 0);
    EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if read_from_scan detects the record deleted by myself, it function
     * returns Status::WARN_ALREADY_DELETE.
     */
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    EXPECT_EQ(Status::WARN_ALREADY_DELETE, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, abort(s));

    /**
     * test
     * if read_from_scan detects the record deleted by others between open_scan
     * and read_from_scan, it function returns Status::WARN_CONCURRENT_DELETE which
     * means reading deleted record.
     */
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, delete_record(s2, storage, k));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::WARN_CONCURRENT_DELETE, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    tuple->get_key(key);
    ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
