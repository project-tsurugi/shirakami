
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

class tsurugi_issue665_2_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue665_2_test");
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

TEST_F(tsurugi_issue665_2_test, // NOLINT
       stall_test) {            // NOLINT
    /**
     * test senario
     * t1: insert a
     * t2: insert b
     * t2: abort
     * (before fix) check gc unhook
     * (after fix) check not unhook while 1 sec
    */
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    // test
    // t1: insert a
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(insert(t1, st, "a", ""));
    Record* rec_ptr{};
    ASSERT_OK(get<Record>(st, "a", rec_ptr));
    ASSERT_EQ(rec_ptr->get_shared_tombstone_count(), 1);
    // t2
    ASSERT_OK(tx_begin({t2, transaction_type::SHORT}));
    ASSERT_OK(insert(t2, st, "a", ""));
    ASSERT_EQ(rec_ptr->get_shared_tombstone_count(), 2);
    ASSERT_OK(abort(t2));
    ASSERT_EQ(rec_ptr->get_shared_tombstone_count(), 1);
#if 0
    // (before fix) check gc unhook
    // sleep for unhook
    Status rc{};
    do {
        std::this_thread::yield();
        Record* rec_ptr{};
        rc = get<Record>(st, "a", rec_ptr);
    } while (rc == Status::OK);
#else
    // (after fix) check not unhook while 1 sec
    ASSERT_OK(get<Record>(st, "a", rec_ptr));
    sleep(1);
    ASSERT_OK(get<Record>(st, "a", rec_ptr));
#endif
    ASSERT_EQ(rec_ptr->get_shared_tombstone_count(), 1);
    ASSERT_OK(commit(t1));
    ASSERT_EQ(rec_ptr->get_shared_tombstone_count(), 0);

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing
