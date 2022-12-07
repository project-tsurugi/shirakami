
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
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class single_long_update_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "single_long_update_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(single_long_update_test, start_before_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK,
                  tx_begin({s,
                            transaction_options::transaction_type::LONG,
                            {st}}));
        ASSERT_EQ(Status::WARN_PREMATURE, update(s, st, "", ""));
    } // start epoch
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_long_update_test,                 // NOLINT
       update_not_existing_key_at_read_phase) { // NOLINT
                                                // preapre
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_long_update_test,                   // NOLINT
       update_not_existing_key_at_commit_phase) { // NOLINT
                                                  // preapre
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, update(s2, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));  // NOLINT
    ASSERT_NE(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_reason_code(),
              reason_code::KVS_UPDATE);
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_key(), "");

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing