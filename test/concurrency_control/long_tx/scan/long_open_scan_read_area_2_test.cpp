
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class long_open_scan_read_area_2_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only-"
                "scan-long_open_scan_read_area_2_test");
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

TEST_F(long_open_scan_read_area_2_test,      // NOLINT
       read_area_empty_negative_not_hit) { // NOLINT
                                           // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {}}}));
    wait_epoch_update();
    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_open_scan_read_area_2_test,      // NOLINT
       read_area_not_empty_negative_hit) { // NOLINT
                                           // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    wait_epoch_update();
    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::ERR_READ_AREA_VIOLATION,
              open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_open_scan_read_area_2_test,          // NOLINT
       read_area_not_empty_negative_not_hit) { // NOLINT
                                               // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    Storage st2{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, create_storage("2", st2));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st2}}}));
    wait_epoch_update();
    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing