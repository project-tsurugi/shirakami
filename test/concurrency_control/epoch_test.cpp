
#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/interface_helper.h"

#include "clock.h"
#include "logger.h"

#include "shirakami/interface.h"

#include "spdlog/spdlog.h"

#include "gtest/gtest.h"

using namespace shirakami;
using namespace shirakami::logger;

namespace shirakami::testing {

class epoch_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override { init(); } // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(epoch_test, sleep_to_watch_change_epoch) { // NOLINT
    setup_spdlog();
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleepMs(PARAM_EPOCH_TIME * 2);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    shirakami_logger->debug("first epoch {0}, second epoch {1}", first, second);
}

TEST_F(epoch_test, check_no_or_one_change_epoch) { // NOLINT
    setup_spdlog();
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleepMs(PARAM_EPOCH_TIME * 2);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    shirakami_logger->debug("first epoch {0}, second epoch {1}", first, second);
    tx_begin(token); // load latest epoch
    first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleepMs(PARAM_EPOCH_TIME * 2);
    /**
     * Epoch increment condition is that all worker load latest epoch. But token's worker is also sleeping 1 sec.
     * So global epoch is changed at most 1.
     */
    second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_EQ(second - first <= 1, true);
    shirakami_logger->debug("first epoch {0}, second epoch {1}", first, second);
    ASSERT_EQ(leave(token), Status::OK);
}

} // namespace shirakami::testing
