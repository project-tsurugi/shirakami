
#include <xmmintrin.h>

#include <bitset>
#include <mutex>

#ifdef WP

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/record.h"

#endif

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

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

TEST_F(insert_after_delete, repeat_insert_search_delete_test1) { // NOLINT
    std::string k("k");                                          // NOLINT
    std::string v("v");                                          // NOLINT
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
#ifndef WP
        std::this_thread::sleep_for(std::chrono::milliseconds{80}); // NOLINT
#endif
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            EXPECT_EQ(Status::OK, insert(s, st, k, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            Record* rec_ptr{};
            ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
            std::string b{};
            rec_ptr->get_key(b);
            ASSERT_EQ(k, b);
            rec_ptr->get_value(b);
            ASSERT_EQ(v, b);
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
    std::string k("k1");                                         // NOLINT
    std::string k2("k2");                                        // NOLINT
    std::string v("v");                                          // NOLINT
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
            std::string vb2{};
            ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
            ASSERT_EQ(Status::OK, delete_record(s, st, k));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1000}); // NOLINT
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            EXPECT_EQ(Status::OK, insert(s, st, k2, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1000}); // NOLINT
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin(s));
            std::string vb{};
            auto rc{search_key(s, st, k, vb)};
            ASSERT_EQ(true, rc == Status::WARN_NOT_FOUND ||
                                    rc == Status::WARN_CONCURRENT_DELETE);
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
    }
}
} // namespace shirakami::testing