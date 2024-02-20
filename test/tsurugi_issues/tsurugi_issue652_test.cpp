
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue652_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue652_test");
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

INSTANTIATE_TEST_SUITE_P(commit_from_one, tsurugi_issue652_test,
                         ::testing::Values(true, false));

TEST_P(tsurugi_issue652_test, // NOLINT
       simple) {              // NOLINT
                              // prepare
    Storage st{};
    ASSERT_OK(create_storage("predicate_test", st));

    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t1, transaction_options::transaction_type::SHORT}));

    ASSERT_OK(upsert(t1, st, "A", "0"));
    ASSERT_OK(upsert(t1, st, "B", "0"));
    ASSERT_OK(upsert(t1, st, "C", "0"));
    ASSERT_OK(commit(t1));

    ASSERT_OK(
            tx_begin({t1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_OK(
            tx_begin({t2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // tx1
    ScanHandle shd{};
    ASSERT_OK(open_scan(t1, st, "B", scan_endpoint::INCLUSIVE, "C",
                        scan_endpoint::INCLUSIVE, shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "B");
    ASSERT_OK(next(t1, shd));
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "C");
    ASSERT_EQ(next(t1, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(t1, shd));
    ASSERT_OK(upsert(t1, st, "A", "1"));

    // tx2
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "A");
    ASSERT_OK(next(t2, shd));
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "B");
    ASSERT_OK(next(t2, shd));
    ASSERT_OK(read_key_from_scan(t2, shd, buf));
    ASSERT_EQ(buf, "C");
    ASSERT_EQ(next(t2, shd), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(t2, shd));
    ASSERT_OK(delete_record(t2, st, "A"));

    // commit
    bool cf1 = GetParam();
    if (cf1) {
        ASSERT_OK(commit(t1));
        ASSERT_OK(commit(t2));
    } else {
        std::atomic<Status> cb_rc{};
        std::atomic<bool> was_committed{false};
        auto cb = [&cb_rc, &was_committed](
                          Status rs, [[maybe_unused]] reason_code rc_og,
                          [[maybe_unused]] durability_marker_type dm) {
            cb_rc.store(rs, std::memory_order_release);
            was_committed = true;
        };
        ASSERT_FALSE(commit(t2, cb)); // boundary wait can't bypass root
        ASSERT_OK(commit(t1));
        while (!was_committed) { _mm_pause(); }
        ASSERT_OK(cb_rc);
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing