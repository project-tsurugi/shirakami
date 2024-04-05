
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class tsurugi_issue438_3_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue438_3");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

TEST_F(tsurugi_issue438_3_test, simple) {
    Storage st1;
    Storage st2;
    ASSERT_OK(create_storage("A", st1));
    ASSERT_OK(create_storage("B", st2));

    Token t1;
    Token t2;
    Token t3;

    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(insert(t1, st1, "0", ""));
    ASSERT_OK(insert(t1, st1, "1", ""));
    ASSERT_OK(insert(t1, st1, "2", ""));
    ASSERT_OK(insert(t1, st1, "3", ""));
    ASSERT_OK(insert(t1, st2, "0", ""));
    ASSERT_OK(insert(t1, st2, "1", ""));
    ASSERT_OK(insert(t1, st2, "2", ""));
    ASSERT_OK(insert(t1, st2, "3", ""));
    ASSERT_OK(commit(t1));

    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st1}, {{}, {st2}}}));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {}, {{}, {st2}}}));
    ASSERT_OK(tx_begin({t3, transaction_type::LONG, {st2}}));
    wait_epoch_update();

    ASSERT_OK(insert(t1, st1, "5", ""));
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(t3, st1, "5", buf));
    ASSERT_OK(insert(t3, st2, "5", ""));

    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_called{false};
    auto cb = [&cb_rc,
               &was_called](Status rs, [[maybe_unused]] reason_code rc,
                            [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        was_called.store(true, std::memory_order_release);
    };

    ASSERT_OK(commit(t1));
    ASSERT_TRUE(commit(t3, cb)); // NOLINT
    ASSERT_OK(cb_rc);

    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
}

} // namespace shirakami::testing
