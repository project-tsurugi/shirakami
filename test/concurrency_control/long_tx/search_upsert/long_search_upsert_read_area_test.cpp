
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

class long_search_upsert_read_area_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-"
                "search_upsert-long_search_upsert_read_area_test_test");
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

TEST_F(long_search_upsert_read_area_test, explicit_read_positive) { // NOLINT
    // ==============================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin({s1,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st}, {}}}));
    ASSERT_EQ(Status::OK, tx_begin({s2,
                                    transaction_options::transaction_type::LONG,
                                    {st},
                                    {{}, {}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s2, st, "", ""));
    // ==============================

    // ==============================
    // test (blind write not wait for ltx)
    ASSERT_EQ(Status::OK, commit(s2));
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

TEST_F(long_search_upsert_read_area_test, universe_read_positive) { // NOLINT
    // ==============================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin({s1,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {}}}));
    ASSERT_EQ(Status::OK, tx_begin({s2,
                                    transaction_options::transaction_type::LONG,
                                    {st},
                                    {{}, {}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s2, st, "", ""));
    // ==============================

    // ==============================
    // test (blind write not wait for other ltx)
    ASSERT_EQ(Status::OK, commit(s2));
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

TEST_F(long_search_upsert_read_area_test, explicit_read_negative) { // NOLINT
    // ==============================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin({s1,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    ASSERT_EQ(Status::OK, tx_begin({s2,
                                    transaction_options::transaction_type::LONG,
                                    {st},
                                    {{}, {}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s2, st, "", ""));
    // ==============================

    // ==============================
    // test
    ASSERT_EQ(Status::OK, commit(s2));
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

TEST_F(long_search_upsert_read_area_test, not_set_read_negative) { // NOLINT
    // ==============================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin({s1,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {}}}));
    ASSERT_EQ(Status::OK, tx_begin({s2,
                                    transaction_options::transaction_type::LONG,
                                    {st},
                                    {{}, {}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s2, st, "", ""));
    // ==============================

    // ==============================
    // test (blind write not wait for other ltx)
    ASSERT_EQ(Status::OK, commit(s2));
    // ==============================

    // ==============================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    // ==============================
}

} // namespace shirakami::testing
