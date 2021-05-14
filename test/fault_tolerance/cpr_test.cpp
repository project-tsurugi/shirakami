
#include <xmmintrin.h> // NOLINT
// It is used, but clang-tidy warn.

#include "concurrency_control/include/session_info.h"

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
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(cpr_test, cpr_action_against_null_db) {  // NOLINT
    std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
    log_dir.append("/build/cpr_test_log");
    init(false, log_dir); // NOLINT
    setup_spdlog();
    register_storage(storage);
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()), false); // null db has no checkpoint.
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    ASSERT_EQ(upsert(token, storage, k, k), Status::OK);
    auto* ti = static_cast<session_info*>(token);
    std::size_t sst_num{};
    if (ti->get_phase() == cpr::phase::REST) {
        sst_num = ti->get_version();
    } else {
        sst_num = ti->get_version() + 1;
    }
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(boost::filesystem::exists(log_dir + "/sst" + std::to_string(sst_num)), true);
    ASSERT_EQ(leave(token), Status::OK);
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
    ASSERT_EQ(search_key(token, storage, k, &tup), Status::OK);
    ASSERT_EQ(std::string(tup->get_key()), k); // NOLINT
    std::string tup_key{tup->get_key()};
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    ASSERT_EQ(delete_record(token, storage, tup_key), Status::OK);
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(leave(token), Status::OK);
    fin();
}

} // namespace shirakami::testing
