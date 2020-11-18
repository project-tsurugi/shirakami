
#include "concurrency_control/silo_variant/include/epoch.h"
#include "concurrency_control/silo_variant/include/interface_helper.h"

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

#if defined(RECOVERY)
TEST_F(epoch_test, clean_up_before_test) {  // NOLINT
    delete_all_records();
    cpr::wait_next_checkpoint();
}

#endif

TEST_F(epoch_test, sleep_to_watch_change_epoch) {  // NOLINT
    setup_spdlog();
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleep(1);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    SPDLOG_DEBUG("first epoch {0}, second epoch {1}", first, second);
}

TEST_F(epoch_test, check_no_or_one_change_epoch) { // NOLINT
    setup_spdlog();
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    epoch::epoch_t first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleep(1);
    epoch::epoch_t second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_NE(first, second);
    SPDLOG_DEBUG("first epoch {0}, second epoch {1}", first, second);
    tx_begin(token); // load latest epoch
    first = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    sleep(1);
    /**
     * Epoch increment condition is that all worker load latest epoch. But token's worker is also sleeping 1 sec.
     * So global epoch is changed at most 1.
     */
    second = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    ASSERT_EQ(second - first <= 1, true);
    SPDLOG_DEBUG("first epoch {0}, second epoch {1}", first, second);
    ASSERT_EQ(leave(token), Status::OK);
}

}  // namespace shirakami::testing
