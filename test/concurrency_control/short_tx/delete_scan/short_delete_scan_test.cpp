
#include <mutex>
#include <thread>

// test/include
#include "test_tool.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class short_delete_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "delete_scan-short_delete_scan_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(short_delete_scan_test,          // NOLINT
       delete_against_open_scan_read) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(upsert(s, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s, st, "b", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "a");
    ASSERT_EQ(Status::OK, delete_record(s2, st, "a"));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "b");
    ASSERT_EQ(Status::ERR_CC, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_delete_scan_test,                                // NOLINT
       delete_against_open_scan_but_not_read_scan_not_read) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(upsert(s, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s, st, "b", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, delete_record(s2, st, "b"));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "a");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_delete_scan_test,                            // NOLINT
       delete_against_open_scan_but_not_read_scan_read) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(upsert(s, st, "a", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, delete_record(s2, st, "a"));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing