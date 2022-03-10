
#include <xmmintrin.h>

#include <bitset>
#include <mutex>

#ifdef WP

#include "concurrency_control/wp/include/epoch.h"

#else

#include "concurrency_control/silo/include/epoch.h"

#endif

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st;

class insert_after_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert-insert_after_delete_test");
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/insert_after_delete_test_log");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(insert_after_delete, independent_tx) { // NOLINT
    register_storage(st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    epoch::epoch_t before_insert{epoch::get_global_epoch()};
    //std::size_t ctr{0};
    while (Status::OK != insert(s, st, k, v)) {
        //LOG_EVERY_N(INFO, 20) << "fail insert"; // NOLINT
        // ^^ NOLINT does not work on Ubuntu18.04. It works in 20.04.
        //if (ctr % 30 == 0) { // NOLINT
        //    LOG(INFO) << "fail insert";
        //}
        //++ctr;
        _mm_pause();
    }
    epoch::epoch_t after_insert{epoch::get_global_epoch()};
    LOG(INFO) << "before_insert " << before_insert;
    LOG(INFO) << "after_insert " << after_insert;
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_after_delete, same_tx) { // NOLINT
    register_storage(st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    LOG(INFO);
    ASSERT_EQ(Status::OK, insert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

TEST_F(insert_after_delete, repeat_insert_search_delete_test1) { // NOLINT
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    register_storage(st);
    {
        Token s{};
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            ASSERT_EQ(Status::OK, insert(s, st, k, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            std::string vb{};
            ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
            ASSERT_EQ(v, vb);
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            ASSERT_EQ(Status::OK, delete_record(s, st, k));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            EXPECT_EQ(Status::OK, insert(s, st, k, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            std::string vb{};
            ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
            ASSERT_EQ(v, vb);
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
    }
}

TEST_F(insert_after_delete, repeat_insert_search_delete_test2) { // NOLINT
    std::string k("k1"); // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v("v"); // NOLINT
    register_storage(st);
    {
        Token s{};
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            ASSERT_EQ(Status::OK, insert(s, st, k, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            std::string vb{}, vb2{};
            ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
            ASSERT_EQ(Status::OK, delete_record(s, st, k));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1000});
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            EXPECT_EQ(Status::OK, insert(s, st, k2, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1000});
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            std::string vb{};
            ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, k, vb));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
    }
}
} // namespace shirakami::testing
