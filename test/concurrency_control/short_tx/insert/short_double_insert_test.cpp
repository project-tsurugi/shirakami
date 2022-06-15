
#include <bitset>
#include <mutex>
#include <thread>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/tuple_local.h"

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
    register_storage(st);
    std::string k("k");
    std::string v("v");
    Token s{};
    {
#ifdef WP
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
#endif
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, insert(s, st, k, v));
        /**
          * this epoch is a.
          */
        ASSERT_EQ(Status::OK, abort(s));
#ifdef WP
        // wp impl can convert deleted to insert.
        Record* rec_ptr{};
        ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
        tid_word tid{loadAcquire(rec_ptr->get_tidw_ref().get_obj())};
        ASSERT_EQ(tid.get_absent(), true);
        ASSERT_EQ(tid.get_latest(), false);
#else
        // wait unhook by background thread
        sleep(1);
#endif
        ASSERT_EQ(Status::OK, insert(s, st, k, v));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

#ifdef WP
TEST_F(double_insert, insert_after_user_abort_not_convert) { // NOLINT
    Storage st{};
    register_storage(st);
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
    sleep(1);
    Record* rec_ptr{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, get<Record>(st, k, rec_ptr));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}
#endif

} // namespace shirakami::testing
