
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class book_18_4 : public ::testing::TestWithParam<transaction_type> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-book_18_4");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

// from Tsurugi Book 18.4

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

INSTANTIATE_TEST_SUITE_P(book_18_4_bool, book_18_4,
    ::testing::Values(transaction_type::SHORT, transaction_type::LONG));

TEST_P(book_18_4, read_only_anomaly) {
    transaction_type tx1_type = GetParam();
    Storage stX;
    Storage stY;
    Token t2;  // LTX
    Token t1;  // OCC or LTX
    Token t3;  // LTX
    VLOG(10) << "setup";
    ASSERT_OK(create_storage("X", stX));
    ASSERT_OK(create_storage("Y", stY));
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(t1));
    ASSERT_OK(insert(t1, stX, "x", "0"));
    ASSERT_OK(insert(t1, stY, "y", "0"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    wait_epoch_update();
    VLOG(10) << "setup done";
    std::string val;

    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {stX}}));
    wait_start_tx(t2);

    VLOG(10) << "r2(x0)";
    ASSERT_OK(search_key(t2, stX, "x", val));
    VLOG(10) << "r2(y0)";
    ASSERT_OK(search_key(t2, stY, "y", val));

    ASSERT_OK(enter(t1));
    transaction_options::write_preserve_type tx1_wp{};
    if (tx1_type == transaction_type::LONG) { tx1_wp = {stY}; }
    ASSERT_OK(tx_begin({t1, tx1_type, tx1_wp}));
    wait_start_tx(t1);

    VLOG(10) << "r1(y0)";
    ASSERT_OK(search_key(t1, stY, "y", val));
    VLOG(10) << "w1(y1)";
    ASSERT_OK(upsert(t1, stY, "y", "1"));

    VLOG(10) << "c1";
    Status t1c_rc;
    std::atomic_bool t1c_done = false;
    commit(t1, [&t1c_done, &t1c_rc](Status rc, [[maybe_unused]] reason_code,
                                    [[maybe_unused]] durability_marker_type) {
        t1c_rc = rc;
        t1c_done = true;
    });

    ASSERT_OK(enter(t3));
    ASSERT_OK(tx_begin({t3, transaction_type::LONG, {}}));
    wait_start_tx(t3);

    VLOG(10) << "r3(x0)";
    ASSERT_OK(search_key(t3, stX, "x", val));
    ASSERT_EQ(val, "0");
    VLOG(10) << "r3(y1)";
    Status r3_rc = search_key(t3, stY, "y", val);
    // case 1: early abort by read
    // case 2: abort at commit(t3)
    if (r3_rc == Status::ERR_CC) {
        VLOG(10) << "   ... ERR_CC";
    } else if (r3_rc == Status::OK) {
        VLOG(10) << "   ... OK";
        VLOG(10) << "c3";
        Status t3c_rc;
        std::atomic_bool t3c_done = false;
        commit(t3, [&t3c_done, &t3c_rc](
            Status rc, [[maybe_unused]] reason_code,
            [[maybe_unused]] durability_marker_type) {
            t3c_rc = rc;
            t3c_done = true;
        });
        while (!t3c_done) { _mm_pause(); }
        ASSERT_EQ(t3c_rc, Status::ERR_CC);
    } else {
        LOG(FATAL) << "unexpected rc:" << r3_rc;
    }
    // t3 aborted

    VLOG(10) << "w2(x2)";
    ASSERT_OK(upsert(t2, stX, "x", "2"));
 
    VLOG(10) << "c2";
    ASSERT_OK(commit(t2));
    ASSERT_OK(leave(t2));

    while (!t1c_done) { _mm_pause(); }

    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t3));

    // cleanup
    ASSERT_OK(delete_storage(stX));
    ASSERT_OK(delete_storage(stY));
}

} // namespace shirakami::testing
