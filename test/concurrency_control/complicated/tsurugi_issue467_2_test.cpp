
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class ti467_2_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue467_2_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
        init_for_test();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(ti467_2_test, // NOLINT
       ng_case1) {   // NOLINT
    // https://github.com/project-tsurugi/tsurugi-issues/issues/467#issuecomment-1867482481
    // 上記コメントを受けて、下記コメントのものを簡潔化
    // https://github.com/project-tsurugi/tsurugi-issues/issues/467#issuecomment-1867258088

    // create storage
    Storage st{};
    ASSERT_OK(create_storage("test", st));

    Token t1{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "1", "0"));
    ASSERT_OK(commit(t1));

    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();

    std::string buf{};
    // ltx search key
    ASSERT_OK(search_key(t1, st, "1", buf));
    ASSERT_EQ(buf, "0");
    sleep(1);

    // rtx begin
    Token t2{};
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::READ_ONLY}));
    wait_epoch_update();

    ASSERT_EQ(static_cast<session*>(t1)->get_valid_epoch(),
              static_cast<session*>(t2)->get_valid_epoch());

    // rtx open scan, it should be ok
    ScanHandle shd{};
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd)); // may fail
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd));

    ASSERT_OK(commit(t2));
    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

TEST_F(ti467_2_test,        // NOLINT
       ng_case2) { // NOLINT
    // https://github.com/project-tsurugi/tsurugi-issues/issues/467#issuecomment-1867258088

    Storage system_st;
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              get_storage("__system_sequences", system_st));
    ASSERT_OK(create_storage("__system_sequences", system_st));
    std::vector<std::string> out_vec;
    ASSERT_OK(list_storage(out_vec));
    ASSERT_EQ(out_vec.size(), 1);

    Token t1{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ScanHandle shd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(t1, system_st, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));

    Storage st{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, get_storage("test", st));
    ASSERT_OK(create_storage("test", st));
    ASSERT_EQ(Status::OK, get_storage("test", st));

    ASSERT_OK(upsert(t1, st, "\x80\x00\x00\x01",
                     "~\x7f\xff\xff\xff\xff\xff\xff\xfe~\xce\xff\xff\xff\xff"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    TxStateHandle thd{};
    ASSERT_OK(acquire_tx_state_handle(t1, thd));
    TxState ts{};
    do {
        ASSERT_OK(check_tx_state(thd, ts));
    } while (ts.state_kind() == TxState::StateKind::WAITING_START);

    // ltx open scan, read key/value from scan
    ASSERT_OK(open_scan(t1, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "\x80\x00\x00\x01");
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "~\x7f\xff\xff\xff\xff\xff\xff\xfe~\xce\xff\xff\xff\xff");
    // ltx search key
    ASSERT_OK(search_key(t1, st, "\x80\x00\x00\x01", buf));
    ASSERT_EQ(buf, "~\x7f\xff\xff\xff\xff\xff\xff\xfe~\xce\xff\xff\xff\xff");
    // ltx delete_record, upsert
    ASSERT_OK(delete_record(t1, st, "\x80\x00\x00\x01"));
    ASSERT_OK(upsert(t1, st, "\x80\x00\x00\x01",
                     "~\x7f\xff\xff\xff\xff\xff\xff\xf6~\xce\xff\xff\xff\xff"));
    sleep(1);
    // ltx next, close
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t1, shd));
    ASSERT_OK(close_scan(t1, shd));

    // rtx begin
    Token t2{};
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::READ_ONLY}));
    TxStateHandle thd2{};
    ASSERT_OK(acquire_tx_state_handle(t2, thd2));
    TxState ts2{};
    ASSERT_OK(check_tx_state(thd, ts));
    ASSERT_EQ(ts.state_kind(), TxState::StateKind::STARTED);

    // rtx open scan, it should be ok
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd)); // may fail
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd));

    ASSERT_OK(commit(t2));
    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing