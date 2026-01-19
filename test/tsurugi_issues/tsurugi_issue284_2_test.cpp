
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;



namespace shirakami::testing {

// for issue#284 problem case2
// setup: make alive record with key=K
// 1. OCC: del at K, commit
// 2. RTX: begin_tx
// 3. OCC: insert at K
// 4. RTX: open_scan
// 5. RTX: next...
// 6. RTX: read_from_scan at K
//    -> may read empty value (bug)
// 7. OCC: commit

class tsurugi_issue284_2_test
    : public ::testing::TestWithParam<std::tuple<
              transaction_options::transaction_type, bool>> { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue284_2_test");
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

INSTANTIATE_TEST_SUITE_P( // NOLINT
        TwoBoolParam, tsurugi_issue284_2_test,
        ::testing::Values(
                std::make_tuple(transaction_options::transaction_type::LONG,
                                true),
                std::make_tuple(transaction_options::transaction_type::LONG,
                                false),
                std::make_tuple(
                        transaction_options::transaction_type::READ_ONLY, true),
                std::make_tuple(
                        transaction_options::transaction_type::READ_ONLY,
                        false)));


TEST_P(tsurugi_issue284_2_test,                      // NOLINT
       read_from_scan_must_not_read_absent_record) { // NOLINT
    if (std::get<0>(GetParam()) == transaction_options::transaction_type::LONG) { GTEST_SKIP() << "LONG is not supported"; }
    auto tx_type = std::get<0>(GetParam());
    bool FLAGS_check_open_scan = std::get<1>(GetParam());
    // setup
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    if (!FLAGS_check_open_scan) { ASSERT_OK(insert(s, st, "0", "val")); }
    ASSERT_OK(insert(s, st, "1", "val"));
    ASSERT_OK(insert(s, st, "2", "val"));
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));

    // wait ss
    auto ce = epoch::get_global_epoch();
    while (ce > epoch::get_cc_safe_ss_epoch()) { _mm_pause(); }

    // setup done

    // OCC delete and commit
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    ASSERT_OK(delete_record(s, st, "1"));
    ASSERT_OK(delete_record(s, st, "2"));
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));

    // LTX/RTX begin
    Token l{};
    ASSERT_OK(enter(l));
    ASSERT_OK(tx_begin({l, tx_type}));
    TxStateHandle sth{};
    ASSERT_OK(acquire_tx_state_handle(l, sth));
    while (true) {
        TxState state;
        ASSERT_OK(check_tx_state(sth, state));
        if (state.state_kind() == TxState::StateKind::STARTED) break;
        _mm_pause();
    }

    //LOG(INFO) << tx_type << " started";

    // OCC insert
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    ASSERT_OK(insert(s, st, "1", "val"));
    ASSERT_OK(insert(s, st, "2", "val"));

    // LTX/RTX: open_scan
    ScanHandle scanh{};
    auto rc = open_scan(l, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        scanh);
    if (!FLAGS_check_open_scan) {
        ASSERT_OK(rc);
        do {
            std::string key;
            std::string val;
            if (rc = read_key_from_scan(l, scanh, key); rc != Status::OK) {
                VLOG(10) << "read_key_from_scan rc:" << rc;
                continue;
            }
            if (rc = read_value_from_scan(l, scanh, val); rc != Status::OK) {
                VLOG(10) << "read_value_from_scan rc:" << rc << " key:<" << key
                         << ">";
                continue;
            }
            VLOG(40) << "read_from_scan OK, key:<" << key << "> value:<" << val
                     << ">";
            EXPECT_EQ(val, "val");
        } while (next(l, scanh) == Status::OK);
        ASSERT_OK(close_scan(l, scanh));
    }

    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));

    ASSERT_OK(commit(l));
    ASSERT_OK(leave(l));
}

} // namespace shirakami::testing
