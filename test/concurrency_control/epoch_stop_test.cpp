
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

class epoch_stop_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-epoch_stop_test");
        FLAGS_stderrthreshold = 0;
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
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        epoch::epoch_t first{epoch::get_global_epoch()};
        sleepMs(epoch::get_global_epoch_time_ms() * 2);
        epoch::epoch_t second{epoch::get_global_epoch()};
        ASSERT_EQ(first, second);
    }
    epoch::epoch_t first{epoch::get_global_epoch()};
    sleepMs(epoch::get_global_epoch_time_ms() * 2);
    epoch::epoch_t second{epoch::get_global_epoch()};
    ASSERT_NE(first, second);
}

TEST_F(epoch_stop_test, ptp) { // NOLINT
    ASSERT_EQ(epoch::ptp_init_val, epoch::get_perm_to_proc());
    epoch::epoch_t ce{};
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ce = epoch::get_global_epoch();
        epoch::set_perm_to_proc(1);
    }
    sleepMs(epoch::get_global_epoch_time_ms() * 2);
    ASSERT_EQ(epoch::get_perm_to_proc(), 0);
    ASSERT_EQ(ce + 1, epoch::get_global_epoch());
    epoch::set_perm_to_proc(epoch::ptp_init_val);
    sleepMs(epoch::get_global_epoch_time_ms() * 2);
    ASSERT_NE(ce + 1, epoch::get_global_epoch());
}

} // namespace shirakami::testing
