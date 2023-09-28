
#include <mutex>

#include "clock.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class epoch_no_stop_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-epoch_no_stop_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(epoch_no_stop_test, sleep_to_watch_change_epoch) { // NOLINT
    epoch::epoch_t first = epoch::get_global_epoch();
    epoch::epoch_t second = epoch::get_global_epoch();
    while (first == second) {
        _mm_pause();
        second = epoch::get_global_epoch();
    }
    LOG(INFO) << "first epoch " << first << ", second epoch " << second;
}

TEST_F(epoch_no_stop_test,                          // NOLINT
       check_progress_of_step_epoch_by_bg_thread) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti{static_cast<session*>(s)};
    auto first_epoch{ti->get_step_epoch()};
    auto second_epoch{ti->get_step_epoch()};
    while (first_epoch == second_epoch) {
        _mm_pause();
        second_epoch = ti->get_step_epoch();
    }
    LOG(INFO) << first_epoch << " " << second_epoch;
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(epoch_no_stop_test,                                   // NOLINT
       check_not_progress_of_step_epoch_if_operating_true) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti{static_cast<session*>(s)};
    ti->set_operating(1);
    auto first_epoch{ti->get_step_epoch()};
    sleepUs(epoch::get_global_epoch_time_us() * 2);
    auto second_epoch{ti->get_step_epoch()};
    ASSERT_EQ(first_epoch, second_epoch);
    LOG(INFO) << first_epoch << " " << second_epoch;
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
