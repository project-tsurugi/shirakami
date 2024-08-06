
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #890: upsert placeholder doesn't refcount, so unhooked by Record-GC even if it is in write set

namespace shirakami::testing {

class tsurugi_issue890_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue890_test");
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

TEST_F(tsurugi_issue890_test, guard_write_set) {
    // expect
    // OCC1: upsert A 1                  -> OK
    // OCC2: insert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: never unhook A before commit/abort OCC1
    // OCC1: commit                      -> OK

    // before ti#890 fix
    // OCC1: upsert A 1                  -> OK
    // OCC2: insert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: unhook A                                <- bug
    // OCC1: commit -> ERR_CC, LOG "unreachable path" <- wrong

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s1, st, "a", "s1"));

    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s2, st, "a", "s2"));
    ASSERT_OK(abort(s2));

    // wait gc
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();
    // placeholder should not be unhooked

    // check unhooked: by (rec_ptr form y::get("a")) == OCC1->write_set[0].rec_ptr
    Record* rec_ptr{};
    EXPECT_EQ(get<Record>(st, "a", rec_ptr), Status::OK);
    auto& s1_ws = ((session*)s1)->get_write_set().get_ref_cont_for_occ();
    ASSERT_EQ(s1_ws.size(), 1);
    EXPECT_EQ(rec_ptr, s1_ws.at(0).get_rec_ptr());

    EXPECT_EQ(commit(s1), Status::OK);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

TEST_F(tsurugi_issue890_test, guard_write_set_case2) {
    // scenario like above test (guard_write_set), works well before ti#890 fix
    // expect
    // OCC1: insert A 1                  -> OK
    // OCC2: upsert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: never unhook A before commit/abort OCC1
    // OCC1: commit                      -> OK

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s1, st, "a", "s1"));

    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s2, st, "a", "s2"));
    ASSERT_OK(abort(s2));

    // wait gc
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();
    // placeholder should not be unhooked

    // check unhooked: by (rec_ptr form y::get("a")) == OCC1->write_set[0].rec_ptr
    Record* rec_ptr{};
    EXPECT_EQ(get<Record>(st, "a", rec_ptr), Status::OK);
    auto& s1_ws = ((session*)s1)->get_write_set().get_ref_cont_for_occ();
    ASSERT_EQ(s1_ws.size(), 1);
    EXPECT_EQ(rec_ptr, s1_ws.at(0).get_rec_ptr());

    EXPECT_EQ(commit(s1), Status::OK);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// regression test
TEST_F(tsurugi_issue890_test, two_wso_of_same_key) {
    // expect
    // OCC1: upsert A 11                 -> OK
    // OCC2: insert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: never unhook A before commit/abort OCC1
    // OCC1: upsert A 12                 -> OK
    //       local write set "A" is overwritten
    // OCC1: commit                      -> OK

    // before ti#890 fix
    // OCC1: upsert A 11                 -> OK
    // OCC2: insert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: unhook A                                <- bug
    // OCC1: upsert A 12                 -> OK
    //       local write set "A" is duplicated        <- bug
    // OCC1: commit -> dead lock                      <- bug

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s1, st, "a", "s1-1"));

    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s2, st, "a", "s2"));
    ASSERT_OK(abort(s2));

    // wait gc
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();
    // placeholder should not be unhooked

    // add write set of same key
    ASSERT_OK(upsert(s1, st, "a", "s1-2"));
    EXPECT_EQ(((session*)s1)->get_write_set().get_ref_cont_for_occ().size(), 1);
    // (before ti#890 fix): write_set (vector) [ {"a", old_rec_ptr, UPSERT "s1-1"}, {"a", new_rec_ptr, UPSERT "s1-2"} ]

    EXPECT_EQ(commit(s1), Status::OK);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// regression test
TEST_F(tsurugi_issue890_test, two_wso_of_same_key_case2) {
    // scenario like above test (two_wso_of_same_key)
    // expect
    // OCC1: upsert A 11                 -> OK
    // OCC2: insert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: never unhook A before commit/abort OCC1
    // OCC1: insert A 12                 -> WARN_ALREADY_EXISTS
    // OCC1: commit                      -> OK

    // before ti#890 fix
    // OCC1: upsert A 11                 -> OK
    // OCC2: insert A 2                  -> OK
    // OCC2: abort                       -> OK
    // RecGC: unhook A                                <- bug
    // OCC1: insert A 12                 -> OK        <- wrong
    //       local write set "A" is duplicated        <- bug
    // OCC1: commit -> dead lock                      <- bug

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s1, st, "a", "s1-1"));

    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s2, st, "a", "s2"));
    ASSERT_OK(abort(s2));

    // wait gc
    wait_epoch_update();
    wait_epoch_update();
    wait_epoch_update();
    // placeholder should not be unhooked

    // add write set of same key
    EXPECT_EQ(insert(s1, st, "a", "s1-2"), Status::WARN_ALREADY_EXISTS);
    EXPECT_EQ(((session*)s1)->get_write_set().get_ref_cont_for_occ().size(), 1);
    // (before ti#890 fix): write_set (vector) [ {"a", old_rec_ptr, UPSERT "s1-1"}, {"a", new_rec_ptr, INSERT "s1-2"} ]

    EXPECT_EQ(commit(s1), Status::OK);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
