
#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_tx_state_metadata_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-tx_state-"
                "long_tx_state_metadata_test");
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

TEST_F(long_tx_state_metadata_test, long_before_after_commit_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG));
    wait_epoch_update();
    auto* ti{static_cast<session*>(s)};
    ASSERT_EQ(false, ti->get_has_current_tx_state_handle());
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    ASSERT_EQ(true, ti->get_has_current_tx_state_handle());
    ASSERT_NE(nullptr, ti->get_current_tx_state_ptr());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(false, ti->get_has_current_tx_state_handle());
    ASSERT_EQ(nullptr, ti->get_current_tx_state_ptr());
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG));
    ASSERT_EQ(false, ti->get_has_current_tx_state_handle());
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    ASSERT_EQ(true, ti->get_has_current_tx_state_handle());
    ASSERT_NE(nullptr, ti->get_current_tx_state_ptr());
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(false, ti->get_has_current_tx_state_handle());
    ASSERT_EQ(nullptr, ti->get_current_tx_state_ptr());
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing