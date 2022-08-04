
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_insert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "long_insert_test");
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

TEST_F(long_insert_test, start_before_epoch) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::LONG, {st}}));

        // test
        ASSERT_EQ(Status::WARN_PREMATURE, insert(s, st, "", ""));

        // cleanup
    }
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_insert_test, start_after_epoch) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, insert(s, st, "", "test"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(buf, "test");

    // cleanup
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
