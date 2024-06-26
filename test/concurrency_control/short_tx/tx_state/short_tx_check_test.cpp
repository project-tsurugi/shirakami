
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_check_tx_state_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-"
                "interface-tx_state-short_check_tx_state_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(short_check_tx_state_test, check_tx_state_not_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, check_tx_state(hd, buf));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_check_tx_state_test, short_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    TxState buf{};
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::ABORTED);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_check_tx_state_test, short_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    TxState buf{};
    {
        stop_epoch();
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
#ifdef PWAL
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_DURABLE);
#else
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
#endif
    }
    resume_epoch();
#ifdef PWAL
    // wait durable
    auto ce = epoch::get_global_epoch();
    for (;;) {
        if (epoch::get_datastore_durable_epoch() >= ce) { break; }
        _mm_pause();
    }
#endif
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
