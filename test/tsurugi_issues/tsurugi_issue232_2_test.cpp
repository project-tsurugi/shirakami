
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class tsurugi_issue232_2 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue232_2");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

// for issue#232 regression test, caused by issue#438

void wait_start_tx(Token tx) {
    TxStateHandle sth{};
    ASSERT_OK(acquire_tx_state_handle(tx, sth));
    while (true) {
        TxState state;
        ASSERT_OK(check_tx_state(sth, state));
        if (state.state_kind() == TxState::StateKind::STARTED) break;
        _mm_pause();
    }
}

TEST_F(tsurugi_issue232_2, case_11) {
    Storage stA;
    Storage stB;
    ASSERT_OK(create_storage("A", stA));
    ASSERT_OK(create_storage("B", stB));

    // CASE_11
    // by using read area, 3rd tx need not wait 2nd tx
    VLOG(10) << "CASE 11";
    Token t1;
    Token t2;
    Token t3;

    VLOG(10) << "TX1: begin ltx wp A";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {stA}}));
    wait_start_tx(t1);
    wait_epoch_update();

    VLOG(10) << "TX2: begin ltx ra ex-B";
    // but TX2 does nothing
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {}, {{}, {stB}}}));
    wait_start_tx(t2);
    wait_epoch_update();

    VLOG(10) << "TX3: begin ltx wp B";
    ASSERT_OK(enter(t3));
    ASSERT_OK(tx_begin({t3, transaction_type::LONG, {stB}}));
    wait_start_tx(t3);

    VLOG(10) << "TX3: read storageA key0 (yellow conflict)";
    std::string val;
    ASSERT_EQ(search_key(t3, stA, "k0", val), Status::WARN_NOT_FOUND);
    VLOG(10) << "TX3: insert storageB key1";
    ASSERT_OK(insert(t3, stB, "k1", "v1"));

    // TX3 commit waiting, because TX3 read stB (t1 WP yellow)
    VLOG(10) << "TX3: commit (wait)";
    ASSERT_EQ(commit(t3), Status::WARN_WAITING_FOR_OTHER_TX);

    VLOG(10) << "TX1: insert storageA key0 (yellow -> red)";
    ASSERT_OK(insert(t1, stA, "k0", "v0"));
    // TX3 waiting bypass, no boudary wait,
    // TX2 don't read stB which TX3 write, so no read wait

    VLOG(10) << "TX1: commit";
    ASSERT_OK(commit(t1));
    auto rc = check_commit(t3);
    while (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
        _mm_pause();
        rc = check_commit(t3);
    }
    // TX1 does not read storageB
    // can TX3 < TX1
    // TX2 never read B (by read area ex-B)
    VLOG(10) << "TX3: commit-waiting -> OK";
    ASSERT_OK(rc);

    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t1));

    ASSERT_OK(abort(t2));
    ASSERT_OK(leave(t2));

    // cleanup
    ASSERT_OK(delete_storage(stA));
    ASSERT_OK(delete_storage(stB));
}

TEST_F(tsurugi_issue232_2, case_12) {
    Storage stA;
    Storage stB;
    ASSERT_OK(create_storage("A", stA));
    ASSERT_OK(create_storage("B", stB));

    // CASE_12
    // by using read area, 3rd tx need not wait 2nd tx
    VLOG(10) << "CASE 12";
    Token t1;
    Token t2;
    Token t3;

    VLOG(10) << "TX1: begin ltx wp A ra ex-B";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {stA}, {{}, {stB}}}));
    wait_start_tx(t1);
    wait_epoch_update();

    VLOG(10) << "TX2: begin ltx ra ex-B";
    // but TX2 does nothing
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {}, {{}, {stB}}}));
    wait_start_tx(t2);
    wait_epoch_update();

    VLOG(10) << "TX3: begin ltx wp B";
    ASSERT_OK(enter(t3));
    ASSERT_OK(tx_begin({t3, transaction_type::LONG, {stB}}));
    wait_start_tx(t3);

    VLOG(10) << "TX1: insert storageA key0";
    ASSERT_OK(insert(t1, stA, "k0", "v0"));
    VLOG(10) << "TX3: read storageA key0 (read conflict)";
    std::string val;
    ASSERT_EQ(search_key(t3, stA, "k0", val), Status::WARN_NOT_FOUND);
    VLOG(10) << "TX3: insert storageB key1";
    ASSERT_OK(insert(t3, stB, "k1", "v1"));

    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_called{false};
    auto cb = [&cb_rc,
               &was_called](Status rs, [[maybe_unused]] reason_code rc,
                            [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        was_called.store(true, std::memory_order_release);
    };

    VLOG(10) << "TX1: commit";
    ASSERT_OK(commit(t1));
    ASSERT_TRUE(commit(t3, cb)); // NOLINT
    ASSERT_OK(cb_rc);

    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t1));
    ASSERT_OK(abort(t2));
    ASSERT_OK(leave(t2));

    // cleanup
    ASSERT_OK(delete_storage(stA));
    ASSERT_OK(delete_storage(stB));
}

} // namespace shirakami::testing
