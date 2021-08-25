
#include <glog/logging.h>

#ifdef WP

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/interface_helper.h"

#else

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/interface_helper.h"

#endif

#include "clock.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class epoch_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override { init(); } // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(epoch_test, sleep_to_watch_change_epoch) { // NOLINT
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleepMs(PARAM_EPOCH_TIME * 2);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
}

TEST_F(epoch_test, check_no_or_one_change_epoch) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleepMs(PARAM_EPOCH_TIME * 2);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
    tx_begin(token); // load latest epoch
    first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleepMs(PARAM_EPOCH_TIME * 2);
    /**
     * Epoch increment condition is that all worker load latest epoch. But token's worker is also sleeping 1 sec.
     * So global epoch is changed at most 1.
     */
    second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_EQ(second - first <= 1, true);
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
    ASSERT_EQ(leave(token), Status::OK);
}

} // namespace shirakami::testing
