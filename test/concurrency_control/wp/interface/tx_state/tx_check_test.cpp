
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class tx_check_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "interface-tx_state-tx_check_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/tx_check_test_log");
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
    static inline std::string log_dir_;       // NOLINT
};

TEST_F(tx_check_test, tx_check_not_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, tx_check(hd, buf));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tx_check_test, short_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, false, false));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    TxState buf{};
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::ABORTED);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tx_check_test, short_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, false, false));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    ASSERT_EQ(Status::OK, commit(s));
    TxState buf{};
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(tx_check_test, long_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    {
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, tx_begin(s, false, true));
        ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
        ASSERT_EQ(Status::OK, tx_check(hd, buf));
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_START);
    }
    wait_change_epoch();
    // first
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    // second should change
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::COMMITTABLE);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::ABORTED);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tx_check_test, long_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    {
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, tx_begin(s, false, true));
        ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
        ASSERT_EQ(Status::OK, tx_check(hd, buf));
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_START);
    }
    wait_change_epoch();
    // first
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    // second should change
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::COMMITTABLE);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
