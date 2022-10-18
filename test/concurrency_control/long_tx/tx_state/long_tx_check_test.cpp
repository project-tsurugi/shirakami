
#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_tx_check_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "interface-tx_state-long_tx_check_test");
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

TEST_F(long_tx_check_test, long_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    {
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::LONG}));
        ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
        ASSERT_EQ(Status::OK, tx_check(hd, buf));
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_START);
    }
    wait_epoch_update();
    // first
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    // second should not change
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::ABORTED);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_check_test, long_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    {
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::LONG}));
        ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
        ASSERT_EQ(Status::OK, tx_check(hd, buf));
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_START);
    }
    wait_epoch_update();
    // first
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    // second should not change without commit api
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    { // acquire epoch lock
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, tx_check(hd, buf));
#ifdef PWAL
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_DURABLE);
#endif
    } // release epoch lock
#ifdef PWAL
    // wait durable
    auto ce = epoch::get_global_epoch();
    for (;;) {
        if (lpwal::get_durable_epoch() >= ce) { break; }
        _mm_pause();
    }
#endif
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
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
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    { // acquire epoch lock
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ASSERT_EQ(Status::OK, tx_check(hd, buf));
#ifdef PWAL
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_DURABLE);
#else
        ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
#endif
    } // release epoch lock
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
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
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
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2)); // NOLINT
    TxState buf{};
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::WAITING_CC_COMMIT);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::COMMITTABLE);
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

} // namespace shirakami::testing
