
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class epoch_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-epoch_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
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

TEST_F(epoch_test, check_progress_of_step_epoch) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti{static_cast<session*>(s)};
    auto first_epoch{ti->get_step_epoch()};
    sleepMs(PARAM_EPOCH_TIME * 2);
    auto second_epoch{ti->get_step_epoch()};
    ASSERT_NE(first_epoch, second_epoch);
    LOG(INFO) << first_epoch << " " << second_epoch;
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
