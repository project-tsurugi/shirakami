
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

class tsurugi_issue249 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue249");
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

TEST_F(tsurugi_issue249, simple) { // NOLINT
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s1, st, "", "a"));
    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_committed{false};
    reason_code rc{};
    auto cb = [&cb_rc, &rc,
               &was_committed](Status rs, reason_code rc_og,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        rc = rc_og;
        was_committed = true;
    };
    ASSERT_TRUE(commit(s1, cb));
    ASSERT_OK(cb_rc);

    // wait cc snap for read only
    sleep(1);
    // 1: tx1 start as RTX
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    // 2: tx1 select for test table
    std::string buf{};
    ASSERT_OK(search_key(s1, st, "", buf));
    ASSERT_EQ(buf, "a");
    // 3: tx2 start LTX wp st
    ASSERT_OK(
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // 4: tx2 update st
    ASSERT_OK(update(s2, st, "", "b"));
    // 5: tx2 commit wihtout wait
    ASSERT_TRUE(commit(s2, cb));
    ASSERT_OK(cb_rc);

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
