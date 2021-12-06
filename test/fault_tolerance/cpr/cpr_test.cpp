
#include <xmmintrin.h> // NOLINT
// It is used, but clang-tidy warn.

#ifdef WP

#include "concurrency_control/wp/include/session.h"

#else

#include "concurrency_control/silo/include/session.h"

#endif

#include "fault_tolerance/include/log.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

Storage storage;

class cpr_test : public ::testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}
};

void list_directory(std::string_view dir) {
    using namespace boost::filesystem;
    path dir_path{std::string(dir)};
    directory_iterator end_itr{};
    std::vector<std::string> entries{};
    for (boost::filesystem::directory_iterator itr(dir_path); itr != end_itr; ++itr) {
        entries.emplace_back(itr->path().string());
    }
    if (entries.empty()) {
        std::cerr << "no file entries." << std::endl;
        return;
    }
    std::sort(entries.begin(), entries.end());
    std::cout << "[list start]" << std::endl;
    for (auto&& e : entries) {
        std::cerr << e << std::endl;
    }
    std::cout << "[list end]" << std::endl;
}

TEST_F(cpr_test, cpr_action_against_null_db) {  // NOLINT
    google::InitGoogleLogging("shirakami-cpr_test");
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/build/cpr_test_log");
    init(false, log_dir); // NOLINT
    register_storage(storage);
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()), false); // null db has no checkpoint.
    {
        Token token{};
        ASSERT_EQ(enter(token), Status::OK);
        {
            std::string k("a"); // NOLINT
            ASSERT_EQ(upsert(token, storage, k, k), Status::OK);
        }
        {
            std::string k("Z"); // NOLINT
            ASSERT_EQ(upsert(token, storage, k, k), Status::OK);
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
        EXPECT_EQ(boost::filesystem::exists(log_dir + "/sst" + std::to_string(sst_num)), true);
        ASSERT_EQ(leave(token), Status::OK);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{230}); // NOLINT
    {
        Token token{};
        ASSERT_EQ(enter(token), Status::OK);
        std::string k("b"); // NOLINT
        ASSERT_EQ(upsert(token, storage, k, k), Status::OK);
        auto* ti = static_cast<session*>(token);
        std::size_t sst_num{};
        if (ti->get_phase() == cpr::phase::REST) {
            sst_num = ti->get_version();
        } else {
            sst_num = ti->get_version() + 1;
        }
        ASSERT_EQ(commit(token), Status::OK); // NOLINT
        cpr::wait_next_checkpoint();
        EXPECT_EQ(boost::filesystem::exists(log_dir + "/sst" + std::to_string(sst_num)), true);
        ASSERT_EQ(leave(token), Status::OK);
    }
    fin(false);
}

TEST_F(cpr_test, cpr_recovery) {                // NOLINT
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/build/cpr_test_log");
    init(true, log_dir); // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    Tuple* tup{};
    ASSERT_EQ(search_key(token, storage, k, tup), Status::OK);
    ASSERT_EQ(std::string(tup->get_key()), "a");   // NOLINT
    ASSERT_EQ(std::string(tup->get_value()), "a"); // NOLINT
    std::string tup_key{tup->get_key()};
    Tuple* tup2{};
    ASSERT_EQ(search_key(token, storage, "b", tup2), Status::OK);
    ASSERT_EQ(std::string(tup2->get_key()), "b");   // NOLINT
    ASSERT_EQ(std::string(tup2->get_value()), "b"); // NOLINT
    ASSERT_EQ(commit(token), Status::OK);           // NOLINT
    ASSERT_EQ(delete_record(token, storage, tup_key), Status::OK);
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(leave(token), Status::OK);
    fin(false); // NOLINT
}

TEST_F(cpr_test, cpr_recovery_again) {                // NOLINT
    // this testcase assumes to be run continuously after ones above
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/build/cpr_test_log");
    init(true, log_dir); // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("b"); // NOLINT
    Tuple* tup{};
    ASSERT_EQ(search_key(token, storage, k, tup), Status::OK);
    ASSERT_EQ(std::string(tup->get_key()), "b");   // NOLINT
    ASSERT_EQ(std::string(tup->get_value()), "b"); // NOLINT
    ASSERT_EQ(commit(token), Status::OK);           // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(leave(token), Status::OK);
    fin(false); // NOLINT
}

} // namespace shirakami::testing
