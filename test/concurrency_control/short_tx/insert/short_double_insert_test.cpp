
#include <bitset>
#include <mutex>
#include <thread>

#include "atomic_wrapper.h"

#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class double_insert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert-double_insert_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(double_insert, insert_after_user_abort) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k");
    std::string v("v");
    Token s{};
    {
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, insert(s, st, k, v));
        /**
          * this epoch is a.
          */
        ASSERT_EQ(Status::OK, abort(s));
        // wp impl can convert deleted to insert.
        Record* rec_ptr{};
        ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
        tid_word tid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        ASSERT_EQ(tid.get_absent(), true);
        ASSERT_EQ(tid.get_latest(), false);
        ASSERT_EQ(Status::OK, insert(s, st, k, v));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(double_insert, insert_after_user_abort_not_convert) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k");
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    /**
      * this epoch is a.
      */
    ASSERT_EQ(Status::OK, abort(s));
    // wait unhook by background thread
    Record* rec_ptr{};
    while (get<Record>(st, k, rec_ptr) != Status::WARN_NOT_FOUND) {
        _mm_pause();
    }
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(double_insert, insert_insert_conflict_commit_commit) { // NOLINT
                                                              // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, insert(s2, st, "", ""));

    // test
    // first inserter win
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    // second inserter lose
    ASSERT_EQ(Status::ERR_KVS, commit(s2)); // NOLINT
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_reason_code(),
              reason_code::KVS_INSERT);
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_key(), "");
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_storage_name(),
              "");
    // warn already exist
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}


TEST_F(double_insert, insert_insert_conflict_abort_commit) { // NOLINT
                                                             // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, insert(s2, st, "", ""));

    // test
    ASSERT_EQ(Status::OK, abort(s1));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // verify
    std::string sb{};
    ASSERT_EQ(Status::OK, search_key(s1, st, "", sb));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing