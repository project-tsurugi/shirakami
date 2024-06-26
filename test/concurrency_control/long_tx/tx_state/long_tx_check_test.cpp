
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_tx_check_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx"
                                  "tx_state-long_tx_check_test");
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

TEST_F(long_tx_check_test, long_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG}));
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

TEST_F(long_tx_check_test, long_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG}));
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

TEST_F(long_tx_check_test, long_tx_started_to_waiting_durable) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG}));
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

TEST_F(long_tx_check_test, long_tx_wait_high_priori_tx) { // NOLINT
    // ==============================
    // prepare
    Token s1{};
    Token s2{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    stop_epoch();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    resume_epoch();
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s2, hd));
    wait_epoch_update();
    // ==============================

    // ==============================
    // test
    // occur forwarding
    std::string sbuf{};
    ASSERT_EQ(Status::OK, search_key(s2, st, "", sbuf));
    // must wait high priori ltx due to forwarding
    stop_epoch();
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2)); // NOLINT
    TxState buf{};
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    ASSERT_TRUE(buf.state_kind() == TxState::StateKind::WAITING_CC_COMMIT);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    // at least, s2 dm is the same to s1 or later
#ifdef PWAL
    for (;;) {
        if (check_commit(s2) == Status::OK) { break; }
        _mm_pause();
    }
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    /**
         * If epoch does not change from commit, this status must be
         * WAITING_DURABLE
         */
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_DURABLE);
    resume_epoch();
    for (;;) {
        _mm_pause();
        ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
        if (buf.state_kind() == TxState::StateKind::DURABLE) { break; }
    }
#else
    for (;;) {
        if (check_commit(s2) == Status::OK) { break; }
        _mm_pause();
    }
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
#endif
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

} // namespace shirakami::testing
