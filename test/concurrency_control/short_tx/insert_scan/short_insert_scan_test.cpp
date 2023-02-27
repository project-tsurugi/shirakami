
#include <bitset>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_insert_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "insert_scan-short_insert_scan_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(short_insert_scan_test, insert_find_phantom) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    // s1 does range read.
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s1, hd));
    // s2 does insert.
    ASSERT_EQ(Status::OK, insert(s2, st, "k", ""));
    // s3 does insert and find phantom
    ASSERT_EQ(Status::ERR_CC, insert(s1, st, "k2", ""));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_insert_scan_test, scan_read_own_insert) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_insert_scan_test, scan_after_insert_committed) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test: insert
    std::string key{"12345678"};
    for (std::size_t i = 0; i < 105; ++i) {
        memcpy(key.data(), &i, sizeof(i));
        ASSERT_EQ(Status::OK, insert(s, st, key, ""));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test: scan
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{"12345678"};
    for (std::size_t i = 0; i < 105; ++i) {
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
        if (i == 104) {
            ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
        } else {
            auto ret = next(s, hd);
            if (Status::OK != ret) { LOG(FATAL) << ret << ", " << i; }
        }
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing