#include <bitset>

#include "concurrency_control/include/tuple_local.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

class insert_delete_scan : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/insert_delete_scan_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

Storage storage;

TEST_F(insert_delete_scan, delete_insert_on_scan) { // NOLINT
    register_storage(storage);
    std::string k("testing");  // NOLINT
    std::string k2("new_key"); // NOLINT
    std::string v("bbb");      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* t{};
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, handle));
#if defined(CPR)
    while (Status::OK != read_from_scan(s, handle, t)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, t));
#endif
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#ifdef CPR
    while (Status::OK != search_key(s, storage, k2, t)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k2, t));
#endif
    ASSERT_TRUE(t); // NOLINT
#ifdef CPR
    Status ret = search_key(s, storage, k, t);
    while (ret == Status::WARN_CONCURRENT_UPDATE) {
        ret = search_key(s, storage, k, t);
    }
#else
    Status ret = search_key(s, storage, k, t);
#endif
    ASSERT_TRUE(ret == Status::WARN_NOT_FOUND || ret == Status::WARN_CONCURRENT_DELETE); // NOLINT
    /**
     * Status::WARN_CONCURRENT_DELETE : Detected records that were deleted but remained in the index so that CPR threads
     * could be found, or read only snapshot transactions could be found.
     */
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
