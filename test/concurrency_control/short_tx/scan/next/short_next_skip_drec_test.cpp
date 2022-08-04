
#include <bitset>
#include <mutex>

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class c_next : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_next_skip_drec_test_test");
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

TEST_F(c_next, next_skip_one_drec) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // the cursor must point k1
    ASSERT_EQ(Status::OK, delete_record(s2, st, k2));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // test
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k3);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(c_next, next_skip_two_drec) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k4, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // the cursor must point k1
    ASSERT_EQ(Status::OK, delete_record(s2, st, k2));
    ASSERT_EQ(Status::OK, delete_record(s2, st, k3));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // test
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k4);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(c_next, next_skip_three_drec) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    std::string k5{"k5"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k4, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k5, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // the cursor must point k1
    ASSERT_EQ(Status::OK, delete_record(s2, st, k2));
    ASSERT_EQ(Status::OK, delete_record(s2, st, k3));
    ASSERT_EQ(Status::OK, delete_record(s2, st, k4));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // test
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(sb, k5);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
