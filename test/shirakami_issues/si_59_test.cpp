
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;



namespace shirakami::testing {

class si_59 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-shirakami_issues-si_59");
        init_for_test();
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

TEST_F(si_59, simple) { // NOLINT
                        // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "a", ""));
    ASSERT_OK(upsert(t1, st, "b", ""));
    ASSERT_OK(upsert(t1, st, "c", ""));
    ASSERT_OK(commit(t1)); // NOLINT

    // test
    // page: a,b,c
    // t1: write c
    // t2: open_scan by full but read a, b
    ASSERT_OK(
            tx_begin({t1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_OK(
            tx_begin({t2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // t2 open_scan, forwarding candidates
    ScanHandle shd{};
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));

    // t2 read a, b
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_OK(next(t2, shd));
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "b");

    // t1 write c, commit
    ASSERT_OK(upsert(t1, st, "c", ""));
    ASSERT_OK(commit(t1)); // NOLINT

    // t2 can commit because open_scan is full but actual is [a:b] so no conflict
    ASSERT_OK(commit(t2)); // NOLINT

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing