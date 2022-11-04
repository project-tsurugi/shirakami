
#include <xmmintrin.h>

#include <bitset>
#include <mutex>

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
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
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(insert_after_delete, repeat_insert_search_delete_test1) { // NOLINT
    std::string k("k");                                          // NOLINT
    std::string v("v");                                          // NOLINT
    create_storage("", st);
    {
        Token s{};
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            ASSERT_EQ(Status::OK, insert(s, st, k, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            std::string vb{};
            ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
            ASSERT_EQ(v, vb);
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            ASSERT_EQ(Status::OK, delete_record(s, st, k));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
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
            ASSERT_EQ(Status::OK, tx_begin({s}));
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
    create_storage("", st);
    {
        Token s{};
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            ASSERT_EQ(Status::OK, insert(s, st, k, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            std::string vb{};
            std::string vb2{};
            ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
            ASSERT_EQ(Status::OK, delete_record(s, st, k));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100}); // NOLINT
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            EXPECT_EQ(Status::OK, insert(s, st, k2, v));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100}); // NOLINT
        {
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, tx_begin({s}));
            std::string vb{};
            auto rc{search_key(s, st, k, vb)};
            ASSERT_EQ(true, rc == Status::WARN_NOT_FOUND);
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
    }
}

TEST_F(insert_after_delete, delete_insert_delete_search) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, insert(s, st, k, v2));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_UPDATE, delete_record(s, st, k));
    std::string vb{};
    ASSERT_EQ(Status::WARN_ALREADY_DELETE, search_key(s, st, k, vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, k, vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing