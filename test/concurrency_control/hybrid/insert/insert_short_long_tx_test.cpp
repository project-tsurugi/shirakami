
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "test_tool.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class insert_short_long_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-upsert_test");
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

TEST_F(insert_short_long_tx_test, longs_insert_after_shorts_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(tx_begin({s2}), Status::OK);
    wait_epoch_update();

    ASSERT_EQ(insert(s2, st, k, v), Status::WARN_CONFLICT_ON_WRITE_PRESERVE);
    ASSERT_EQ(insert(s1, st, k, v), Status::OK);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(insert_short_long_tx_test, longs_insert_after_shorts_upsert) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    std::string k{"k"};
    std::string_view vs{"vs"};
    std::string_view vl{"vl"};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(tx_begin({s2}), Status::OK);
    wait_epoch_update();

    std::string v{};
    ASSERT_EQ(upsert(s2, st, k, vs), Status::OK);
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s1, st, k, v));
    ASSERT_EQ(Status::OK, commit(s2));
    wait_epoch_update();
    //ASSERT_EQ(Status::OK, search_key(s1, st, k, v)); EXPECT_EQ(v, vs);
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s1, st, k, v));
    ASSERT_EQ(insert(s1, st, k, vl), Status::WARN_ALREADY_EXISTS);

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));

    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin({s2}));
    ASSERT_OK(search_key(s2, st, k, v));
    EXPECT_EQ(v, vs);
    ASSERT_OK(commit({s2}));
    ASSERT_OK(leave(s2));
}

TEST_F(insert_short_long_tx_test, shorts_insert_after_longs_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(tx_begin({s1, // NOLINT
                        transaction_options::transaction_type::LONG,
                        {st}}),
              Status::OK);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(tx_begin({s2}), Status::OK); // NOLINT
    wait_epoch_update();

    ASSERT_EQ(insert(s1, st, k, v), Status::OK);
    ASSERT_EQ(insert(s2, st, k, v), Status::WARN_CONFLICT_ON_WRITE_PRESERVE);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(insert_short_long_tx_test, shorts_upsert_after_longs_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    std::string k{"k"};
    std::string_view vs{"vs"};
    std::string_view vl{"vl"};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(tx_begin({s1, // NOLINT
                        transaction_options::transaction_type::LONG,
                        {st}}),
              Status::OK);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(tx_begin({s2}), Status::OK); // NOLINT
    wait_epoch_update();

    ASSERT_EQ(insert(s1, st, k, vl), Status::OK);
    ASSERT_EQ(upsert(s2, st, k, vs), Status::OK);

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
    wait_epoch_update();

    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin({s2}));
    std::string v{};
    ASSERT_OK(search_key(s2, st, k, v));
    EXPECT_EQ(v, vs);
    ASSERT_OK(commit({s2}));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
