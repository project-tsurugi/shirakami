
#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_tx_check_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "interface-tx_state-short_tx_check_test");
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

TEST_F(short_tx_check_test, tx_check_not_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    TxStateHandle hd{};
    TxState buf{};
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, tx_check(hd, buf));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_tx_check_test, short_tx_road_to_abort) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));
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

TEST_F(short_tx_check_test, short_tx_road_to_commit) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    ASSERT_EQ(Status::OK, commit(s));
#ifdef PWAL
    // wait durable
    auto ce = epoch::get_global_epoch();
    for (;;) {
        if (lpwal::get_durable_epoch() >= ce) { break; }
        _mm_pause();
    }
#endif
    TxState buf{};
    ASSERT_EQ(Status::OK, tx_check(hd, buf));
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::DURABLE);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing