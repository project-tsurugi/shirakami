
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_check_tx_state_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-"
                "interface-tx_state-read_only_check_tx_state_test");
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

TEST_F(read_only_check_tx_state_test, read_only_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};

    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_START);
    resume_epoch();
    wait_epoch_update();
    // first
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    // second should not change
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::ABORTED);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_check_tx_state_test, read_only_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_START);
    resume_epoch();
    wait_epoch_update();
    // first
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    // second should not change without commit api
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    stop_epoch();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
#ifdef PWAL
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_DURABLE);
#endif
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

TEST_F(read_only_check_tx_state_test,             // NOLINT
       read_only_tx_started_to_waiting_durable) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    wait_epoch_update();
    TxState buf{};
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    stop_epoch();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
#ifdef PWAL
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_DURABLE);
#else
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
#endif
    resume_epoch();
}

} // namespace shirakami::testing