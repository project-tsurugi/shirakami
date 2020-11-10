
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
    sleep(1);
}

}  // namespace shirakami::testing
