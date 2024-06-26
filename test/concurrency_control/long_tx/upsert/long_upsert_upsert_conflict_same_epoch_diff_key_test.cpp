
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

class long_upsert_upsert_conflict_same_epoch_diff_key_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-"
                "upsert-long_upsert_upsert_conflict_same_epoch_diff_key_test");
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

TEST_F(long_upsert_upsert_conflict_same_epoch_diff_key_test, // NOLINT
       diff_key_same_epoch_io_high_low_co_high_low) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert different (unique) key to the same storage.
     * Insert order high low.
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

    std::string pk1{"pk1"};
    std::string pk2{"pk2"};
    ASSERT_EQ(upsert(s1, st, pk1, ""), Status::OK);
    ASSERT_EQ(upsert(s2, st, pk2, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_upsert_upsert_conflict_same_epoch_diff_key_test, // NOLINT
       diff_key_same_epoch_io_low_high_co_high_low) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert different (unique) key to the same storage.
     * Insert order low high.
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

    std::string pk1{"pk1"};
    std::string pk2{"pk2"};
    ASSERT_EQ(upsert(s2, st, pk2, ""), Status::OK);
    ASSERT_EQ(upsert(s1, st, pk1, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_upsert_upsert_conflict_same_epoch_diff_key_test, // NOLINT
       diff_key_same_epoch_io_high_low_co_low_high) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert different (unique) key to the same storage.
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

    std::string pk1{"pk1"};
    std::string pk2{"pk2"};
    ASSERT_EQ(upsert(s1, st, pk1, ""), Status::OK);
    ASSERT_EQ(upsert(s2, st, pk2, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_upsert_upsert_conflict_same_epoch_diff_key_test, // NOLINT
       diff_key_same_epoch_io_low_high_co_low_high) {        // NOLINT
    /**
     * There are two long tx.
     * They are same epoch.
     * They insert different (unique) key to the same storage.
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

    std::string pk1{"pk1"};
    std::string pk2{"pk2"};
    ASSERT_EQ(upsert(s2, st, pk2, ""), Status::OK);
    ASSERT_EQ(upsert(s1, st, pk1, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
