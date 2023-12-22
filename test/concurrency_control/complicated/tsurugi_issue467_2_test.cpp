
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
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(ti467_2_test, // NOLINT
       ng_case) {    // NOLINT
    // https://github.com/project-tsurugi/tsurugi-issues/issues/467#issuecomment-1867258088

    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));

    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "\x80\x00\x00\x01", "0"));
    ASSERT_OK(commit(t1));

    // ng test
    // ltx begin
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();

    // ltx open scan, read key/value from scan
    ScanHandle shd{};
    ASSERT_OK(open_scan(t1, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "\x80\x00\x00\x01");
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "0");
    // ltx search key
    ASSERT_OK(search_key(t1, st, "\x80\x00\x00\x01", buf));
    ASSERT_EQ(buf, "0");
    // ltx delete_record, upsert
    ASSERT_OK(delete_record(t1, st, "\x80\x00\x00\x01"));
    ASSERT_OK(upsert(t1, st, "\x80\x00\x00\x01", "1"));
    // ltx next, close
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t1, shd));
    ASSERT_OK(close_scan(t1, shd));

    // rtx begin
    ASSERT_OK(tx_begin({t2, transaction_type::READ_ONLY}));

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