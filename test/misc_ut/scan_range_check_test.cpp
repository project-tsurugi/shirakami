
#include "concurrency_control/interface/scan/include/scan.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class scan_range_check_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-misc_ut-scan_range_check_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(scan_range_check_test, regular) {
    // regular non-empty pattern
    // ["a", "a"]; X = "a"
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::INCLUSIVE, "a", scan_endpoint::INCLUSIVE));
    // ["a", "b"];
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::INCLUSIVE, "b", scan_endpoint::INCLUSIVE));
    // ("a", "b")
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::EXCLUSIVE, "b", scan_endpoint::EXCLUSIVE));
    // ["a", "b")
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::INCLUSIVE, "b", scan_endpoint::EXCLUSIVE));
    // ("a", "b"]
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::EXCLUSIVE, "b", scan_endpoint::INCLUSIVE));
    // ["a", inf)
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF));
    // ("a", inf)
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF));
    // (-inf, "b"]
    EXPECT_EQ(Status::OK, check_empty_scan_range("", scan_endpoint::INF, "b", scan_endpoint::INCLUSIVE));
    // (-inf, "b")
    EXPECT_EQ(Status::OK, check_empty_scan_range("", scan_endpoint::INF, "b", scan_endpoint::EXCLUSIVE));
    // (-inf, inf); full scan
    EXPECT_EQ(Status::OK, check_empty_scan_range("", scan_endpoint::INF, "", scan_endpoint::INF));

    // regular empty pattern
    // ["a", "a")
    EXPECT_EQ(Status::WARN_NOT_FOUND, check_empty_scan_range("a", scan_endpoint::INCLUSIVE, "a", scan_endpoint::EXCLUSIVE));

    // valid but rare pattern
    // ["", inf); X >= ""; ie. full scan
    EXPECT_EQ(Status::OK, check_empty_scan_range("", scan_endpoint::INCLUSIVE, "a", scan_endpoint::INF));
    // ("", inf); X > ""
    EXPECT_EQ(Status::OK, check_empty_scan_range("", scan_endpoint::EXCLUSIVE, "a", scan_endpoint::INF));
    // (-inf, ""); X < ""
    EXPECT_EQ(Status::WARN_NOT_FOUND, check_empty_scan_range("", scan_endpoint::INF, "", scan_endpoint::EXCLUSIVE));
    // (-inf, ""]; X <= ""; ie. X = ""
    EXPECT_EQ(Status::OK, check_empty_scan_range("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE));
}

TEST_F(scan_range_check_test, irregular) {
    // invalid range
    // ["b", "a"]; left > right
    EXPECT_EQ(Status::WARN_NOT_FOUND, check_empty_scan_range("b", scan_endpoint::INCLUSIVE, "a", scan_endpoint::INCLUSIVE));
    // ("a", "a"); empty with two EXCLUSIVEs
    EXPECT_EQ(Status::WARN_NOT_FOUND, check_empty_scan_range("a", scan_endpoint::EXCLUSIVE, "a", scan_endpoint::EXCLUSIVE));

    // legal, but not legitimate
    // inf with non empty key; treated these as normal inf
    EXPECT_EQ(Status::OK, check_empty_scan_range("b", scan_endpoint::INF, "a", scan_endpoint::INCLUSIVE));
    EXPECT_EQ(Status::OK, check_empty_scan_range("b", scan_endpoint::INCLUSIVE, "a", scan_endpoint::INF));
    EXPECT_EQ(Status::OK, check_empty_scan_range("b", scan_endpoint::INF, "a", scan_endpoint::INF));
    EXPECT_EQ(Status::OK, check_empty_scan_range("a", scan_endpoint::INF, "a", scan_endpoint::INF));
}

} // namespace shirakami::testing
