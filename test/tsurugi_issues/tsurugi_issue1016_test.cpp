
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #1016: upserting the Record (which is written in same epoch) may not change tid_word

namespace shirakami::testing {

class tsurugi_issue1016_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tsurugi_issues-tsurugi_issue1016_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

TEST_F(tsurugi_issue1016_test, modify_by_upsert_must_change_tidword) {
    // expect
    // OCC1: begin                       -> OK
    // OCC2: begin                       -> OK
    // OCC3: begin                       -> OK
    // wait epoch; to do next 3 lines (OCC1 commit - OCC3 commit) in the same epoch
    // OCC1: upsert A 1, commit          -> OK
    // OCC2: search_key A                -> OK, reads A=1
    // OCC3: upsert A 3, commit          -> OK
    // OCC2: commit                      -> ERR_CC by read verify

    // before ti#1016 fix
    // OCC1: begin                       -> OK
    // OCC2: begin                       -> OK
    // OCC3: begin                       -> OK
    // wait epoch; to do next 3 lines (OCC1 commit - OCC3 commit) in the same epoch
    // OCC1: upsert A 1, commit          -> OK
    // OCC2: search_key A                -> OK, reads A=1
    // OCC3: upsert A 3                  -> OK
    // OCC3: commit                      -> OK  <- sometime use same tid as previous
    // OCC2: commit                      -> OK  <- wrong, read_verify doesn't work

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    Token s3{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));
    ASSERT_OK(enter(s3));

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s3, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    ASSERT_OK(upsert(s1, st, "A", "1"));
    ASSERT_OK(commit(s1));

    std::string buf{};
    ASSERT_OK(search_key(s2, st, "A", buf));
    ASSERT_EQ(buf, "1");

    ASSERT_OK(upsert(s3, st, "A", "3"));
    ASSERT_OK(commit(s3));

    ASSERT_EQ(commit(s2), Status::ERR_CC);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
    ASSERT_OK(leave(s3));
}

} // namespace shirakami::testing
