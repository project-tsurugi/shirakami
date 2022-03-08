
#include <mutex>

#include "concurrency_control/wp/include/ongoing_tx.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class ongoing_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "ongoing_tx_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/ongoing_tx_test_log");
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
    static inline std::string log_dir_; // NOLINT
};

TEST_F(ongoing_tx_test, exist_preceding_id_test) { // NOLINT
    ongoing_tx::push({1, 1});
    ASSERT_EQ(ongoing_tx::exist_preceding_id(2), true);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::exist_preceding_id(2), false);
}

TEST_F(ongoing_tx_test, exist_id_test) { // NOLINT
    ASSERT_EQ(ongoing_tx::exist_id(1), false);
    ongoing_tx::push({1, 1});
    ASSERT_EQ(ongoing_tx::exist_id(1), true);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::exist_id(1), false);
}

TEST_F(ongoing_tx_test, get_lowest_epoch_test) { // NOLINT

    // register
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 0);
    ongoing_tx::push({1, 1});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::push({2, 2});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::push({3, 3});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);

    // delete from old
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 2);
    ongoing_tx::remove_id(2);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 3);
    ongoing_tx::remove_id(3);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 0);

    // register again
    ongoing_tx::push({1, 1});
    ongoing_tx::push({2, 2});
    ongoing_tx::push({3, 3});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);

    // delete from new
    ongoing_tx::remove_id(3);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::remove_id(2);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 0);
}

} // namespace shirakami::testing
