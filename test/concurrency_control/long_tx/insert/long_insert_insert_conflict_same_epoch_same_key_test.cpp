
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_insert_insert_conflict_same_epoch_same_key_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-"
                "insert-long_insert_insert_conflict_same_epoch_same_key_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};


TEST_F(long_insert_insert_conflict_same_epoch_same_key_test, // NOLINT
       same_key_same_epoch_io_low_high_co_high_low) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert same key to the same storage.
     * Insert order is low to high.
     * Commit order is 1. high priority tx, 2. low priority tx.
     */

    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    stop_epoch();
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    resume_epoch();
    wait_epoch_update();

    ASSERT_EQ(insert(s2, st, "", ""), Status::OK);
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::ERR_CC, commit(s2));
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_reason_code(),
              reason_code::KVS_INSERT);
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_key(), "");
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_storage_name(),
              "");
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_insert_insert_conflict_same_epoch_same_key_test, // NOLINT
       same_key_same_epoch_io_high_low_co_high_low) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert same key to the same storage.
     * Insert order is low to high.
     * Commit order is 1. high priority tx, 2. low priority tx.
     */

    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    stop_epoch();
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    resume_epoch();
    wait_epoch_update();

    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(insert(s2, st, "", ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::ERR_CC, commit(s2));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_insert_insert_conflict_same_epoch_same_key_test, // NOLINT
       same_key_same_epoch_io_high_low_co_low_high) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert same key to the same storage.
     * Insert order high low.
     * Commit order is 1. low priority tx, 2. high priority tx.
     */

    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    stop_epoch();
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    resume_epoch();
    wait_epoch_update();

    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(insert(s2, st, "", ""), Status::OK);

    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    Status rc{};
    do {
        rc = check_commit(s2);
        _mm_pause();
    } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
    ASSERT_EQ(Status::ERR_CC, rc);
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_insert_insert_conflict_same_epoch_same_key_test, // NOLINT
       same_key_same_epoch_io_low_high_co_low_high) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert same key to the same storage.
     * Insert order low high.
     * Commit order is 1. low priority tx, 2. high priority tx.
     */

    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    stop_epoch();
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    resume_epoch();
    wait_epoch_update();

    ASSERT_EQ(insert(s2, st, "", ""), Status::OK);
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);

    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    Status rc{};
    do {
        rc = check_commit(s2);
        _mm_pause();
    } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
    ASSERT_EQ(Status::ERR_CC, rc);
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
