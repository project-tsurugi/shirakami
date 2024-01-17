
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"

#include "database/include/logging.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue556_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue556_test");
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

TEST_F(tsurugi_issue556_test, test) { // NOLINT
    // prepare storage
    Storage st1{};
    Storage st2{};
    ASSERT_OK(create_storage("A", st1));
    ASSERT_OK(create_storage("B", st2));

    // prepare session
    Token s1{};
    Token s2{};
    Token s3{};
    /**
     * wp st1
    */
    ASSERT_OK(enter(s1));
     /**
      * read st1,2, wp st2, finally no conflict with s1, but bypassed by s3
     */
    ASSERT_OK(enter(s2));
    /**
     * read st2, wait for s2 bypass s2 to s1. same at s1, wp and write st2, 
     * read wait s2, s2 read st2, commit s3 try. wp and write st2 but s2 read 
     * st2 at future
    */
    ASSERT_OK(enter(s3));

    // prepare t0
    ASSERT_OK(tx_begin({s1, transaction_type::SHORT}));
    ASSERT_OK(upsert(s1, st1, "0", ""));
    ASSERT_OK(upsert(s1, st1, "1", ""));
    ASSERT_OK(upsert(s1, st2, "0", ""));
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_type::LONG, {st1}}));
    wait_epoch_update();
    ASSERT_OK(tx_begin({s2, transaction_type::LONG, {st2}}));
    wait_epoch_update();
    ASSERT_OK(tx_begin({s3, transaction_type::LONG, {st2}}));
    wait_epoch_update();

    // s2 read st1 (without conflict),2 chain
    std::string buf{};
    ASSERT_OK(search_key(s2, st1, "1", buf));
    ASSERT_OK(search_key(s2, st2, "0", buf));
    // s3 read st2, chain
    ASSERT_OK(search_key(s3, st2, "0", buf));

    // s3 write st2
    ASSERT_OK(upsert(s3, st2, "0", ""));

    // s4 commit, boundary wait for s1,2,3 but will bypass 2,3.
    // read wait for s1,2,3, will (can) not bypass
    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_called{false};
    auto cb = [&cb_rc,
               &was_called](Status rs, [[maybe_unused]] reason_code rc,
                            [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        was_called.store(true, std::memory_order_release);
    };

    ASSERT_FALSE(commit(s3, cb));
    // sleep for coming waiting bypass
    sleep(1); // it may need more depending on environment.
    // boundary wait is expected to skip 2,3, but read wait for s1,2,3
    // for s1 boundary, need conflict between s1 and s4

    // s1 write st1 ""
    ASSERT_OK(upsert(s1, st1, "0", "")); // make conflict between s1 and s3
    ASSERT_OK(commit(s1));

    // s2 commit without write, so chain between s2, s3 can be treated as invalid.
    ASSERT_OK(commit(s2));

    // now, s3 bypass 2 and same at s1. read wait can release. wait for commit
    while (!was_called) { _mm_pause(); }

    if (ongoing_tx::get_optflag_disable_waiting_bypass()) {
        LOG(INFO) << "disabled wb";
        /**
         * if no waiting bypass, 3 didn't conflict 2. 3 conflict 1 but 3 don't
         * break 1's read.
        */
       ASSERT_OK(cb_rc); 
    } else {
        LOG(INFO) << "enabled wb";
        /**
         * if waiting bypass, 3 didn't conflict 2 but bypass 2. 3 conflict 1 
         * but 3 don't break 1's read. however, bypassed 3's write break 2's 
         * read (st2, "0"). so 3 fail.
        */
       ASSERT_EQ(cb_rc, Status::ERR_CC); 
    }

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
    ASSERT_OK(leave(s3));
}

} // namespace shirakami::testing