
#include <glog/logging.h>

#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"

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
    epoch::epoch_t first = epoch::get_global_epoch();
    sleepMs(PARAM_EPOCH_TIME * 3);
    epoch::epoch_t second = epoch::get_global_epoch();
    ASSERT_NE(first, second);
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
}

TEST_F(epoch_test, check_no_or_one_change_epoch) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    epoch::epoch_t first = epoch::get_global_epoch();
    sleepMs(PARAM_EPOCH_TIME * 2);
    epoch::epoch_t second = epoch::get_global_epoch();
    ASSERT_NE(first, second);
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
    tx_begin(token); // load latest epoch
    first = epoch::get_global_epoch();
    sleepMs(PARAM_EPOCH_TIME * 2);
    /**
     * Epoch increment condition is that all worker load latest epoch. But token's worker is also sleeping 1 sec.
     * So global epoch is changed at most 1.
     */
    second = epoch::get_global_epoch();
    ASSERT_NE(first, second);
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
    ASSERT_EQ(leave(token), Status::OK);
}

TEST_F(epoch_test, stop_epoch) { // NOLINT
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        epoch::epoch_t first{epoch::get_global_epoch()};
        sleepMs(PARAM_EPOCH_TIME * 2);
        epoch::epoch_t second{epoch::get_global_epoch()};
        ASSERT_EQ(first, second);
    }
    epoch::epoch_t first{epoch::get_global_epoch()};
    sleepMs(PARAM_EPOCH_TIME * 2);
    epoch::epoch_t second{epoch::get_global_epoch()};
    ASSERT_NE(first, second);
}

} // namespace shirakami::testing
