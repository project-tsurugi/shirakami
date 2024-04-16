
#include <mutex>

#include "clock.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class epoch_stop_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-epoch_stop_test");
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

TEST_F(epoch_stop_test, stop_epoch) { // NOLINT
    stop_epoch();
    {
        epoch::epoch_t first{epoch::get_global_epoch()};
        sleepUs(epoch::get_global_epoch_time_us() * 2);
        epoch::epoch_t second{epoch::get_global_epoch()};
        LOG(INFO) << first;
        LOG(INFO) << second;
    }
    resume_epoch();
    epoch::epoch_t first{epoch::get_global_epoch()};
    sleepUs(epoch::get_global_epoch_time_us() * 4);
    epoch::epoch_t second{epoch::get_global_epoch()};
    LOG(INFO) << first;
    LOG(INFO) << second;
}

TEST_F(epoch_stop_test, ptp) { // NOLINT
    ASSERT_EQ(epoch::ptp_init_val, epoch::get_perm_to_proc());
    epoch::epoch_t ce{};
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ce = epoch::get_global_epoch();
        epoch::set_perm_to_proc(1);
        LOG(INFO) << ce;
    }
    sleepUs(epoch::get_global_epoch_time_us() * 4);
    LOG(INFO) << epoch::get_perm_to_proc();
    LOG(INFO) << epoch::get_global_epoch();
    epoch::set_perm_to_proc(epoch::ptp_init_val);
    sleepUs(epoch::get_global_epoch_time_us() * 4);
    LOG(INFO) << epoch::get_global_epoch();
}

} // namespace shirakami::testing
