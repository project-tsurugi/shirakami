
#include <xmmintrin.h>

#include "fault_tolerance/include/log.h"

#ifdef CPR

#include "fault_tolerance/include/cpr.h"

#endif

#include "logger.h"

#include "kvs/interface.h"

#include "gtest/gtest.h"

using namespace shirakami::cc_silo_variant;
using namespace shirakami::logger;

namespace shirakami::testing {

class cpr_test : public ::testing::Test {
public:
    void SetUp() override { init(); } // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(cpr_test, cpr_action_against_null_db) {  // NOLINT
    setup_spdlog();
    delete_all_records();
    sleep(1);
    while (cpr::global_phase_version::get_gpv().get_version() == 0) _mm_pause(); // wait for first checkpoint.
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()), false); // null db has no checkpoint.
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    ASSERT_EQ(upsert(token, k, k), Status::OK);
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    sleep(1);
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()), true); // non-null db has checkpoint.
    ASSERT_EQ(leave(token), Status::OK);
}

#if defined(RECOVERY)

TEST_F(cpr_test, cpr_recovery) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    Tuple* tup{};
    ASSERT_EQ(search_key(token, k, &tup), Status::OK);
    ASSERT_EQ(std::string(tup->get_key()), k); // NOLINT
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    ASSERT_EQ(leave(token), Status::OK);
}

#endif

}  // namespace shirakami::testing
