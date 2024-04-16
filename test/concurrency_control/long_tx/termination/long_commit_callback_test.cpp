
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

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;
using namespace std::chrono_literals;

class long_commit_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "termination-long_commit_callback_test");
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

TEST_F(long_commit_test, commit_long_premature) { // NOLINT
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    std::atomic_bool called = false;
    Status s{};
    auto b = commit(s1, [&](auto st, auto, auto) {
        called = true;
        s = st;
    }); // NOLINT
    ASSERT_TRUE(b);
    ASSERT_TRUE(called);
    ASSERT_EQ(Status::WARN_PREMATURE, s);
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(long_commit_test, commit_long_not_begin) { // NOLINT
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    std::atomic_bool called = false;
    Status s{};
    auto b = commit(s1, [&](auto st, auto, auto) {
        called = true;
        s = st;
    }); // NOLINT
    ASSERT_TRUE(b);
    ASSERT_TRUE(called);
    ASSERT_EQ(Status::WARN_NOT_BEGIN, s);
    ASSERT_EQ(Status::OK, leave(s1));
}
} // namespace shirakami::testing
