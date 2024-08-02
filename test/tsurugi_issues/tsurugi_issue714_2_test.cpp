
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #714 (2):
// tombstone record is unhooked by Record-GC even if it is in local read-set.
// read-verify compares the saved tid with the tid of Record that is unhooked and stored in garbage::container_rec,
// so not be aware of the newly created Record for the same key.
// to fix: (a) guard Records in local read-set from Record-GC,
// or      (b) adjust read-verify to work even if Record is unhooked.

namespace shirakami::testing {

class tsurugi_issue714_2_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-complicated-tsurugi_issue714_2_test");
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

TEST_F(tsurugi_issue714_2_test, must_detect_the_change_of_tombstone) {
    // Setup: make tombstone at "A"

    // expect
    // OCC1: read "A"     -> NOT_FOUND
    // RecGC: run
    // OCC2: insert "A" 2 -> OK
    // OCC2: commit       -> OK
    // OCC1: commit       -> ERR_CC

    // before fix ti#714(2):
    // OCC1: read "A"     -> NOT_FOUND
    // RecGC: run
    // OCC2: insert "A" 2 -> OK
    // OCC2: commit       -> OK
    // OCC1: commit       -> OK  <- bug

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // stop gc
    std::unique_lock<std::mutex> lk{garbage::get_mtx_cleaner()};

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s1, st, "0", "0"));
    ASSERT_OK(insert(s1, st, "a", "0"));
    ASSERT_OK(commit(s1));
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(delete_record(s1, st, "a"));
    ASSERT_OK(commit(s1));
    wait_epoch_update();  // wait next epoch after delete
    // setup done

    std::string str;
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(search_key(s1, st, "a", str), Status::WARN_NOT_FOUND);

    // start gc
    lk.unlock();

    // wait gc
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();

    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s2, st, "a", "2"));
    ASSERT_OK(commit(s2));

    EXPECT_EQ(commit(s1), Status::ERR_CC);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

TEST_F(tsurugi_issue714_2_test, reading_tombstone_not_interrupted_by_recgc) {
    // read tombstone and no one modified this

    // Setup: make tombstone at "A"
    // expect
    // OCC1: read "A"     -> NOT_FOUND
    // RecGC: may run
    // OCC1: commit       -> OK

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    ASSERT_OK(enter(s1));

    // stop gc
    std::unique_lock<std::mutex> lk{garbage::get_mtx_cleaner()};

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s1, st, "0", "0"));
    ASSERT_OK(insert(s1, st, "a", "0"));
    ASSERT_OK(commit(s1));
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(delete_record(s1, st, "a"));
    ASSERT_OK(commit(s1));
    wait_epoch_update();  // wait next epoch after delete
    // setup done

    std::string str;
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(search_key(s1, st, "a", str), Status::WARN_NOT_FOUND);

    // start gc
    lk.unlock();

    // wait gc
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();

    EXPECT_EQ(commit(s1), Status::OK);

    ASSERT_OK(leave(s1));
}

} // namespace shirakami::testing
