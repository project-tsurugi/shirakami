
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class ti467_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue467_test");
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

TEST_F(ti467_test,           // NOLINT
       by_comment_ok_case) { // NOLINT
    // https://github.com/project-tsurugi/tsurugi-issues/issues/467#issuecomment-1857177573

    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));

    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "", "0"));
    ASSERT_OK(commit(t1));

    // ok test
    // 1. ltx begin
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    // 2. rtx begin
    ASSERT_OK(tx_begin({t2, transaction_type::READ_ONLY}));
    wait_epoch_update();

    // 3. ltx update
    ASSERT_OK(update(t1, st, "", "1"));

    // 4. rtx select == 1 entry
    ScanHandle shd{};
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_value_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "0");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd));
    ASSERT_OK(commit(t2));
    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

TEST_F(ti467_test,           // NOLINT
       by_comment_ng_case) { // NOLINT
    // https://github.com/project-tsurugi/tsurugi-issues/issues/467#issuecomment-1857177573

    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));

    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "", "0"));
    ASSERT_OK(commit(t1));

    // ok test
    // 1. ltx begin
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();

    // 2. ltx update
    ASSERT_OK(update(t1, st, "", "1"));

    // 3. rtx begin
    ASSERT_OK(tx_begin({t2, transaction_type::READ_ONLY}));
    wait_epoch_update();

    // 4. rtx select == 1 entry
    ScanHandle shd{};
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_value_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "0");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd));
    ASSERT_OK(commit(t2));
    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing
