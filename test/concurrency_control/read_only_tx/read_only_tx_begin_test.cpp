
#include <array>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_tx_begin_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "helper-read_only_tx_begin_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_only_tx_begin_test, tx_begin_read_only_and_wp) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION,
              tx_begin(s, TX_TYPE::READ_ONLY, {st}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_tx_begin_test, double_tx_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::READ_ONLY));
    wait_epoch_update();
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_begin(s, TX_TYPE::READ_ONLY));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_tx_begin_test, tx_begin_SS_epoch) { // NOLINT
    Token s1{};
    Token s2{};
    Token s3{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, enter(s3));

    ASSERT_EQ(Status::OK, tx_begin(s1, TX_TYPE::LONG));
    LOG(INFO) << static_cast<session*>(s1)->get_valid_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK, tx_begin(s2, TX_TYPE::LONG));
    LOG(INFO) << static_cast<session*>(s2)->get_valid_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK, tx_begin(s3, TX_TYPE::READ_ONLY));
    LOG(INFO) << static_cast<session*>(s3)->get_valid_epoch();

    ASSERT_NE(static_cast<session*>(s1)->get_valid_epoch(),
              static_cast<session*>(s2)->get_valid_epoch());
    ASSERT_NE(static_cast<session*>(s2)->get_valid_epoch(),
              static_cast<session*>(s3)->get_valid_epoch());
    ASSERT_EQ(static_cast<session*>(s1)->get_valid_epoch(),
              static_cast<session*>(s3)->get_valid_epoch());

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s3));
}

} // namespace shirakami::testing