
#include <xmmintrin.h> // NOLINT
// It is used, but clang-tidy warn.

#include <mutex>

#include "concurrency_control/silo/include/session.h"
#include "concurrency_control/silo/include/session_table.h"

#include "fault_tolerance/include/log.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

Storage storage;

class cpr_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-fault_tolerance-cpr-cpr_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
        log_dir_ = "/tmp/shirakami-test-fault_tolerance-cpr-cpr_test";
    }

    void SetUp() override { std::call_once(init_google_, call_once_f); }

    void TearDown() override {}

    static std::string get_log_dir() { return log_dir_; }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(cpr_test, cpr_action_against_null_db) { // NOLINT
    init(false, get_log_dir());                // NOLINT
    register_storage(storage);
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()),
              false); // null db has no checkpoint.
    {
        Token token{};
        ASSERT_EQ(enter(token), Status::OK);
        {
            std::string k("a"); // NOLINT
            std::string v("A"); // NOLINT
            ASSERT_EQ(upsert(token, storage, k, v), Status::OK);
        }
        {
            std::string k("Z"); // NOLINT
            std::string v("z"); // NOLINT
            ASSERT_EQ(upsert(token, storage, k, v), Status::OK);
        }

        auto* ti = static_cast<session*>(token);
        std::size_t sst_num{};
        if (ti->get_phase() == cpr::phase::REST) {
            sst_num = ti->get_version();
        } else {
            sst_num = ti->get_version() + 1;
        }
        ASSERT_EQ(commit(token), Status::OK); // NOLINT
        cpr::wait_next_checkpoint();
        EXPECT_EQ(boost::filesystem::exists(get_log_dir() + "/sst" + // NOLINT
                                            std::to_string(sst_num)),
                  true);
        ASSERT_EQ(leave(token), Status::OK);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{230}); // NOLINT
    {
        Token token{};
        ASSERT_EQ(enter(token), Status::OK);
        std::string k("b"); // NOLINT
        std::string v("B"); // NOLINT
        ASSERT_EQ(upsert(token, storage, k, v), Status::OK);
        auto* ti = static_cast<session*>(token);
        std::size_t sst_num{};
        if (ti->get_phase() == cpr::phase::REST) {
            sst_num = ti->get_version();
        } else {
            sst_num = ti->get_version() + 1;
        }
        ASSERT_EQ(commit(token), Status::OK); // NOLINT
        cpr::wait_next_checkpoint();
        EXPECT_EQ(boost::filesystem::exists(get_log_dir() + "/sst" + // NOLINT
                                            std::to_string(sst_num)),
                  true);
        ASSERT_EQ(leave(token), Status::OK);
    }
    fin(false);
}

TEST_F(cpr_test, cpr_recovery) { // NOLINT
    init(true, get_log_dir());   // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    Tuple* tup{};
    ASSERT_EQ(search_key(token, storage, "a", tup), Status::OK);
    std::string key{};
    tup->get_key(key);
    ASSERT_EQ(key, "a"); // NOLINT
    std::string val{};
    tup->get_value(val);
    ASSERT_EQ(val, "A"); // NOLINT
    std::string tup_key{};
    tup->get_key(tup_key);
    Tuple* tup2{};
    ASSERT_EQ(search_key(token, storage, "b", tup2), Status::OK);
    tup2->get_key(key);
    ASSERT_EQ(key, "b"); // NOLINT
    tup2->get_value(val);
    ASSERT_EQ(val, "B");                  // NOLINT
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    ASSERT_EQ(delete_record(token, storage, tup_key), Status::OK);
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(leave(token), Status::OK);
    fin(false); // NOLINT
}

TEST_F(cpr_test, cpr_recovery_again) { // NOLINT
    // this testcase assumes to be run continuously after ones above
    init(true, get_log_dir()); // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    Tuple* tup{};
    ASSERT_EQ(search_key(token, storage, "b", tup), Status::OK);
    std::string key{};
    tup->get_key(key);
    ASSERT_EQ(key, "b"); // NOLINT
    std::string val{};
    tup->get_value(val);
    ASSERT_EQ(val, "B");                  // NOLINT
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    ASSERT_EQ(leave(token), Status::OK);
    fin(false); // NOLINT
}

} // namespace shirakami::testing
