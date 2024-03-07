
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue284 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue284");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

// for issue#284
// Th1. OCC delete, commit, OCC insert, commit
// Th2. RTX (or LTX) full-scan, read_value_from_scan() may return empty string (data corrupted)

constexpr std::string_view common_val = "0";

std::string mk_key(int i) {
    std::ostringstream ss;
    //ss << std::setw(7) << std::setfill('0') << i;
    ss << std::setw(11) << std::setfill('0') << i; // NOLINT
    return ss.str();
}

TEST_F(tsurugi_issue284, 20230525_comment_ban) { // NOLINT
    int n = 2000;                                // NOLINT

    // setup: make
    //   1 ... n: all values are "0"
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    for (int i = 1; i <= n; i++) {
        ASSERT_OK(insert(s, st, mk_key(i), common_val));
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));

    std::atomic<bool> done = false;
    LOG(INFO) << "start";

    auto work_del_ins = [&st, n, &done]() {
        int i = n / 2 + 1;
        while (!done) {
            Token s{};
            auto key = mk_key(i);
            ASSERT_OK(enter(s));
            ASSERT_OK(tx_begin(
                    {s, transaction_options::transaction_type::SHORT}));
            ASSERT_OK(delete_record(s, st, key));
            ASSERT_OK(commit(s));
            ASSERT_OK(leave(s));
            ASSERT_OK(enter(s));
            ASSERT_OK(tx_begin(
                    {s, transaction_options::transaction_type::SHORT}));
            ASSERT_OK(insert(s, st, key, common_val));
            ASSERT_OK(commit(s));
            ASSERT_OK(leave(s));
            i++;
            if (i >= n) i = 1;
        }
    };
    auto work_scan = [&st, &done]() {
        for (int i = 0; i < 2; i++) {
            Token s{};
            ASSERT_OK(enter(s));
            tx_begin({s, transaction_options::transaction_type::READ_ONLY});
            //tx_begin({s, transaction_options::transaction_type::LONG});
            TxStateHandle sth{};
            ASSERT_OK(acquire_tx_state_handle(s, sth));
            while (true) {
                TxState state;
                ASSERT_OK(check_tx_state(sth, state));
                if (state.state_kind() == TxState::StateKind::STARTED) {
                    break;
                }
                _mm_pause();
            }
            ScanHandle scanh{};
            ASSERT_OK(open_scan(s, st, "", scan_endpoint::INF, "",
                                scan_endpoint::INF, scanh));
            do {
                std::string key;
                std::string val;
                ASSERT_OK(read_key_from_scan(s, scanh, key));
                ASSERT_OK(read_value_from_scan(s, scanh, val));
                VLOG(40) << "key:<" << key << "> value:<" << val << ">";
                if (val != common_val) {
                    LOG_FIRST_N(ERROR, 1) << "try:" << i << " key:<" << key
                                          << "> value:<" << val << ">";
                }
            } while (next(s, scanh) == Status::OK);
            ASSERT_OK(leave(s));
            VLOG(30) << "PASS " << i;
        }
        done = true;
    };

    std::thread t1{work_del_ins};
    std::thread t2{work_scan};
    t1.join();
    t2.join();
}

} // namespace shirakami::testing