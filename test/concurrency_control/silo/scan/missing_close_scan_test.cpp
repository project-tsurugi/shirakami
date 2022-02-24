
#include <mutex>

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

/**
 * @brief test to verify lack of close_scan doesn't cause side effect outside the scan handle
 * @details the lack of close_scan once caused re-reading wrong data on different testcase and running read_first affected read_second,
 * though separately running the testcases were successful.
 */
class missing_close_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-scan-open_scan_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/open_scan_test_log");
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(true, log_dir_+std::to_string(testcase_number_++)); // NOLINT
    }

    void TearDown() override { fin(false); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
    static inline std::size_t testcase_number_{};
};

TEST_F(missing_close_scan_test, read_first) { // NOLINT
    Storage storage0{};
    register_storage(storage0);
    Token s{};
    {
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, tx_begin(s));
        ASSERT_EQ(Status::OK, insert(s, storage0, "a", ""));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, leave(s));
    }
    {
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, tx_begin(s));
        ScanHandle handle{};
        ASSERT_EQ(Status::OK, open_scan(s, storage0, "", scan_endpoint::INF, "",
                scan_endpoint::INF, handle));
        ASSERT_EQ(0, handle);

        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ("a", sb);
        ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
        ASSERT_EQ("", sb);
//        ASSERT_EQ(Status::OK, close_scan(s, handle)); // lack of close_scan caused mis-use of garbage scan handle
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, leave(s));
    }
}

TEST_F(missing_close_scan_test, read_second) { // NOLINT
    Storage storage0{};
    register_storage(storage0);
    Token s{};
    {
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, tx_begin(s));
        ASSERT_EQ(Status::OK, insert(s, storage0, "a", "A"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, leave(s));
    }
    {
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, tx_begin(s));
        ScanHandle handle{};
        ASSERT_EQ(Status::OK, open_scan(s, storage0, "", scan_endpoint::INF, "",
                scan_endpoint::INF, handle));
        ASSERT_EQ(0, handle);

        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ("a", sb);
        ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
        ASSERT_EQ("A", sb);
        ASSERT_EQ(Status::OK, close_scan(s, handle));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, leave(s));
    }
}
} // namespace shirakami::testing
