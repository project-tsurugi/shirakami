
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_search_upsert_wait_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-"
                "search_upsert-long_search_upsert_wait_test_test");
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

TEST_F(long_search_upsert_wait_test, wait_for_overwrite) { // NOLINT
    // ==============================
    // prepare
    Token s1{};
    Token s2{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    std::string sb{};
    // occur forwarding
    ASSERT_EQ(Status::OK, search_key(s2, st, "", sb));
    // ==============================

    // ==============================
    // test
    // wait for overwrite
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2));
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

TEST_F(long_search_upsert_wait_test, wait_for_preceding_lg_later_bd) { // NOLINT
    // test: wait for preceding long tx having later boundary

    // ==============================
    // prepare
    Token s1{};
    Token s2{};
    Token s3{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, enter(s3));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s3, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    std::string sb{};
    ASSERT_EQ(Status::OK, search_key(s3, st, "", sb));
    ASSERT_EQ(Status::OK, upsert(s3, st, "", ""));
    // occur forwarding
    // boundary order: s1 = s3 < s2
    ASSERT_EQ(Status::OK, commit(s1));
    // boundary order: s3 < s2
    // ==============================

    // ==============================
    // test
    // wait for s2 which may execute read
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s3));
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s3));
    // ==============================
}

} // namespace shirakami::testing
