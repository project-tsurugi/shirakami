
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_tx_state_handle_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("long_tx_state_handle_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

// running two testcases below consecutively caused acquiring same state handle
TEST_F(long_tx_state_handle_test, preparation) {  // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    TxStateHandle hd2{};
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, acquire_tx_state_handle(s, hd2));
    ASSERT_EQ(hd, hd2);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_state_handle_test, tx_begin_from_different_session) {  // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin(s1, TX_TYPE::LONG));
    ASSERT_EQ(Status::OK, tx_begin(s2, TX_TYPE::LONG));
    TxStateHandle hd1{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s1, hd1));
    TxStateHandle hd2{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s2, hd2));
    ASSERT_NE(hd1, hd2);

    ASSERT_EQ(Status::OK, release_tx_state_handle(hd1));
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd2));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
