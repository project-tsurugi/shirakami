
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class insert_delete_scan : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "delete_insert_scan_test");
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/insert_delete_scan_test_log");
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
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
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, "",
                                    scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k2, vb));
    ASSERT_NE("", vb); // NOLINT
    Status ret = search_key(s, storage, k, vb);
    ASSERT_TRUE(ret == Status::WARN_NOT_FOUND ||
                ret == Status::WARN_CONCURRENT_DELETE); // NOLINT
    /**
     * Status::WARN_CONCURRENT_DELETE : Detected records that were deleted but remained in the index so that CPR threads
     * could be found, or read only snapshot transactions could be found.
     */
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
