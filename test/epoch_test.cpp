
#include "concurrency_control/silo_variant/include/epoch.h"

#include "logger.h"

#include "kvs/interface.h"

#include "spdlog/spdlog.h"

#include "gtest/gtest.h"

using namespace shirakami::cc_silo_variant;
using namespace shirakami::logger;

namespace shirakami::testing {

class epoch_test : public ::testing::Test {  // NOLINT
public:
    void SetUp() override { init(); } // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(epoch_test, sleep_to_watch_change_epoch) {  // NOLINT
    setup_spdlog();
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleep(1);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    SPDLOG_DEBUG("first epoch {0}, second epoch {1}", first, second);
}

}  // namespace shirakami::testing
