
#include "gtest/gtest.h"
#include "glog/logging.h"

#include "concurrency_control/include/tid.h"
#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class tid_word_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tid_word-tid_word_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

// moved from tx_id_test
TEST_F(tid_word_test, comp) {
    tid_word ltid{0}, stid{0};
    // short, ltx. epoch 1, tid0, by_short test
    ltid.set_by_short(false);
    stid.set_by_short(true);
    ASSERT_GT(stid, ltid);
    // common: tid 0, ltx: epoch 2, occ: epoch 1
    ltid.set_epoch(2);
    stid.set_epoch(1);
    ASSERT_GT(ltid, stid);
    // common: tid 0, ltx: epoch 1, occ: epoch 2
    ltid.set_epoch(1);
    stid.set_epoch(2);
    ASSERT_GT(stid, ltid);
}

TEST_F(tid_word_test, epoch_is_stronger_than_tid) {
    tid_word t1{}, t2{};
    t1.set_epoch(1);
    t1.set_tid(2);
    t2.set_epoch(2);
    t2.set_tid(1);
    EXPECT_LT(t1, t2);
}

TEST_F(tid_word_test, epoch_keeps_ordered) {
    // check epoch=1 < epoch=2 < epoch=4 < ...
    tid_word t1{}, t2{};
    t1.set_tid(0);
    t2.set_tid(0);
    for (epoch::epoch_t e = 1; e; e <<= 1) {
        t1.set_epoch(e);
        t2.set_epoch(e << 1);
        if (t2.get_epoch() == 0) { break; }
        ASSERT_LT(t1.get_epoch(), t2.get_epoch());
        EXPECT_LT(t1, t2);
    }
}

TEST_F(tid_word_test, tid_keeps_ordered) {
    // check tid=1 < tid=2 < tid=4 < ...
    tid_word t1{}, t2{};
    t1.set_epoch(0);
    t2.set_epoch(0);
    for (uint64_t t = 1; t; t <<= 1) {
        t1.set_tid(t);
        t2.set_tid(t << 1);
        if (t2.get_tid() == 0) { break; }
        ASSERT_LT(t1.get_tid(), t2.get_tid());
        EXPECT_LT(t1, t2);
    }
}

} // namespace shirakami::testing
