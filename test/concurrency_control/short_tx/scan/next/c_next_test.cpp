
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
                                  "scan-c_next_test_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(c_next, next_with_not_begin) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_BEGIN, next(s, {}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(c_next, next_with_invalid_handle) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, next(s, {}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(c_next, next_for_two) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    auto* ti = static_cast<session*>(s);
    auto before_next{ti->get_scan_handle().get_scan_cache_itr()[hd]};
    ASSERT_EQ(Status::OK, next(s, hd));
    auto after_next{ti->get_scan_handle().get_scan_cache_itr()[hd]};
    ASSERT_NE(before_next, after_next);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
