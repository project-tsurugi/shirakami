
#include <glog/logging.h>

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

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

using namespace shirakami;

class tsurugi_issue307 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue307");
        FLAGS_stderrthreshold = 0;
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
// Th2. RTX (or LTX) full-scan, read_value_from_scan() may return empty string
// (data corrupted)

constexpr std::string_view common_val = "0";

std::string mk_key(int i) {
    std::ostringstream ss;
    //ss << std::setw(7) << std::setfill('0') << i;
    ss << std::setw(11) << std::setfill('0') << i;
    return ss.str();
}

TEST_F(tsurugi_issue307, DISABLED_simple) { // NOLINT
    // DEFINE_int32(records, 2000, "number of records");
    // DEFINE_int32(rounds, 10000, "number of scan rounds");
    // DEFINE_int32(mod_thread, 1, "number of delete/insert threads");
    // DEFINE_int32(scan_thread, 6, "number of scan threads");
    // DEFINE_bool(read_only, false, "use RTX for scan (if false, use LTX)");

    constexpr int n = 10000;
    constexpr int rounds = 50000;
    constexpr int mod_thread = 1;
    constexpr int scan_thread = 6;
    constexpr bool read_only = true;

    // setup: make
    //   1 ... n: all values are "0"
    Storage st;
    ASSERT_OK(create_storage("", st));
    Token s;
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    for (int i = 1; i <= n; i++) {
        ASSERT_OK(insert(s, st, mk_key(i), common_val));
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));
    wait_epoch_update();

    std::atomic<bool> done = false;
    LOG(INFO) << "start";

    auto work_del_ins = [mod_thread, &st, n, &done](int tid) {
        int i = tid;
        while (!done) {
            Token s{};
            auto key = mk_key(i + 1);
            ASSERT_OK(enter(s));
            ASSERT_OK(tx_begin(s));
            if (auto rc = delete_record(s, st, key); rc != Status::OK) {
                LOG(FATAL);
            }
            if (auto rc = commit(s); rc != Status::OK) { LOG(FATAL); }
            ASSERT_OK(leave(s));
            ASSERT_OK(enter(s));
            ASSERT_OK(tx_begin(s));
            if (auto rc = insert(s, st, key, common_val); rc != Status::OK) {
                LOG(FATAL);
            }
            if (auto rc = commit(s); rc != Status::OK) { LOG(FATAL); }
            ASSERT_OK(leave(s));
            i += mod_thread;
            if (i >= n) i = tid;
        }
    };
    auto tx_type = read_only ? transaction_options::transaction_type::READ_ONLY
                             : transaction_options::transaction_type::LONG;
    auto work_scan = [n, rounds, &st, &done, tx_type]() {
        for (int i = 0; i < rounds; i++) {
            Token s{};
            ASSERT_OK(enter(s));
            tx_begin({s, tx_type});
            TxStateHandle sth{};
            ASSERT_OK(acquire_tx_state_handle(s, sth));
            while (true) {
                TxState state;
                ASSERT_OK(check_tx_state(sth, state));
                if (state.state_kind() == TxState::StateKind::STARTED) break;
                _mm_pause();
            }
            ScanHandle scanh{};
            if (auto rc = open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, scanh);
                rc != Status::OK) {
                LOG(FATAL);
            }
            int count = 0;
            do {
                std::string key;
                std::string val;
                if (auto rc = read_key_from_scan(s, scanh, key);
                    rc != Status::OK) {
                    LOG(FATAL);
                }
                if (auto rc = read_value_from_scan(s, scanh, val);
                    rc != Status::OK) {
                    LOG(FATAL);
                }
                if (val != common_val) { LOG(FATAL); }
                count++;
            } while (next(s, scanh) == Status::OK);
            if (auto rc = commit(s); rc != Status::OK) { LOG(FATAL); }
            if (count != n && count != n - 1) { LOG(FATAL) << count; }
            ASSERT_OK(leave(s));
        }
        done = true;
    };

    std::vector<std::thread> threads;

    threads.reserve(mod_thread + scan_thread);
    for (int i = 0; i < mod_thread; i++) {
        threads.emplace_back(work_del_ins, i);
    }

    for (int i = 0; i < scan_thread; i++) { threads.emplace_back(work_scan); }

    for (auto& e : threads) { e.join(); }
}