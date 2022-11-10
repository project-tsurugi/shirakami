
#include <mutex>
#include <thread>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class scan_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-scan_upsert_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

// start: one tx test
TEST_F(scan_upsert, range_read_after_upsert_same_tx) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(upsert(s, st, "", ""), Status::OK);
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(scan_upsert, upsert_after_range_read_same_tx) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare
    for (auto i = 0; i < 14; ++i) {
        std::string key(1, i);
        std::string value(std::to_string(i));
        ASSERT_EQ(upsert(s, st, key, value), Status::OK);
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    // read all range
    for (auto i = 0; i < 14; ++i) {
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(sb, std::string(1, i));
        ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
        ASSERT_EQ(sb, std::to_string(i));
        if (i != 13) {
            ASSERT_EQ(next(s, handle), Status::OK);
        } else {
            ASSERT_EQ(next(s, handle), Status::WARN_SCAN_LIMIT);
        }
    }
    ASSERT_EQ(close_scan(s, handle), Status::OK);

    std::string key(1, 14);
    std::string value(std::to_string(14));
    ASSERT_EQ(upsert(s, st, key, value), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

// end: one tx test

// start: serial two tx
TEST_F(scan_upsert, range_read_tx_after_upsert_tx) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

// end: serial two tx

} // namespace shirakami::testing