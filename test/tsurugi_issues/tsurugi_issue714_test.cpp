
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue714_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue714_test");
        FLAGS_stderrthreshold = 0;
        init_for_test();
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue714_test, // NOLINT
       simple) {              // NOLINT
    /**
     * issue senario
    */
    // prepare
    Storage st_a{};
    Storage st_b{};
    Storage st_c{};
    ASSERT_OK(create_storage("a", st_a));
    ASSERT_OK(create_storage("b", st_b));
    ASSERT_OK(create_storage("c", st_c));
    Token t1{};
    Token t2{};
    Token t3{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));

    // test
    // t1
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st_b}}));
    ltx_begin_wait(t1);
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(t1, st_a, "1", buf));
    ASSERT_OK(insert(t1, st_b, "1", ""));

    // t2
    ASSERT_OK(tx_begin({t2, transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(t2, st_c, "1", buf));
    ASSERT_OK(insert(t2, st_a, "1", ""));
    ASSERT_OK(commit(t2));
    // t1 < t2

    // t3
    ASSERT_OK(tx_begin({t3, transaction_type::LONG, {st_c}}));
    ltx_begin_wait(t3);
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(t3, st_b, "1", buf));
    // t3 < t1 < t2
    ASSERT_OK(insert(t3, st_c, "1", ""));
    // t3 write c will break committed t2 read c *1
    std::atomic<Status> cb_rc3{};
    std::atomic<bool> was_committed3{false};
    reason_code rc3{};
    auto cb3 = [&cb_rc3, &was_committed3,
                &rc3](Status rs, [[maybe_unused]] reason_code rc,
                      [[maybe_unused]] durability_marker_type dm) {
        cb_rc3.store(rs, std::memory_order_release);
        was_committed3 = true;
        rc3 = rc;
    };
    ASSERT_FALSE(commit(t3, cb3));

    ASSERT_OK(commit(t1));

    while (!was_committed3) { std::this_thread::yield(); }

    // t3 commit find *1
    ASSERT_EQ(cb_rc3, Status::ERR_CC);
    ASSERT_EQ(rc3, reason_code::CC_LTX_PHANTOM_AVOIDANCE);

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
}

TEST_F(tsurugi_issue714_test, DISABLED_shortx) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    
    // test
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(tx_begin({t2, transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(t1, st, "a", buf));
    ASSERT_OK(insert(t2, st, "a", ""));
    ASSERT_OK(commit(t2));
    ASSERT_EQ(Status::ERR_CC, commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing