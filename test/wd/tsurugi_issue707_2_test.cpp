
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

class tsurugi_issue707_2_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue707_2_test");
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

TEST_F(tsurugi_issue707_2_test, // NOLINT
       simple) {                // NOLINT
    // comment for https://github.com/project-tsurugi/tsurugi-issues/issues/707#issuecomment-2027394041
    /**
     * test senario
     * t1: insert a as tombstone
     * epoch update
     * t2: insert a as sharing tombstone
     * t1: abort
     * wait for unhook
     * t2: commit
     * check existance
    */
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    // test
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    LOG(INFO) << static_cast<session*>(t1)->get_begin_epoch();
    wait_epoch_update();
    wait_epoch_update();
    ASSERT_OK(upsert(t1, st, "a", ""));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    LOG(INFO) << static_cast<session*>(t2)->get_begin_epoch();
    wait_epoch_update();
    ASSERT_OK(upsert(t2, st, "a", ""));
    ASSERT_OK(abort(t1));
    // wait for unhook
    sleep(1);
    // check hooking
    Record* rec_ptr{};
    ASSERT_OK(get<Record>(st, "a", rec_ptr));
    // t2 commit
    ASSERT_OK(commit(t2));
    // check existance
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    std::string buf{};
    ASSERT_OK(search_key(t1, st, "a", buf));
    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing