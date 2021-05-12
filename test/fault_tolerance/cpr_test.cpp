
#include <xmmintrin.h> // NOLINT
// It is used, but clang-tidy warn.

#include "fault_tolerance/include/log.h"

#include "logger.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

using namespace shirakami;
using namespace shirakami::logger;

namespace shirakami::testing {

Storage storage;

class cpr_test : public ::testing::Test {
public:
    void SetUp() override {
        register_storage(storage);
    }

    void TearDown() override { fin(); }
};

TEST_F(cpr_test, cpr_action_against_null_db) {  // NOLINT
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/test/tid_test_log");
    init(false, log_dir); // NOLINT
    setup_spdlog();
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()), false); // null db has no checkpoint.
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    ASSERT_EQ(upsert(token, storage, k, k), Status::OK);
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(boost::filesystem::exists(Log::get_kLogDirectory() + "/sst0"), true);
    ASSERT_EQ(leave(token), Status::OK);
}

#if 0
TEST_F(cpr_test, cpr_recovery) {                // NOLINT
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/test/tid_test_log");
    init(true, log_dir); // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    Tuple* tup{};
    ASSERT_EQ(search_key(token, k, &tup), Status::OK);
    ASSERT_EQ(std::string(tup->get_key()), k); // NOLINT
    ASSERT_EQ(commit(token), Status::OK);      // NOLINT
    ASSERT_EQ(delete_record(token, tup->get_key()), Status::OK);
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(leave(token), Status::OK);
}
#endif

TEST_F(cpr_test, cpr_bound) { // NOLINT
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/test/tid_test_log");
    init(false, log_dir); // NOLINT
    setup_spdlog();
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    ASSERT_EQ(leave(token), Status::OK);
}

} // namespace shirakami::testing
