
#include "test_tool.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_check_ltx_is_highest_priority_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-tx_state-long_"
                "check_ltx_is_highest_priority_test");
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

TEST_F(long_check_ltx_is_highest_priority_test, // NOLINT
       simple) {                                // NOLINT
                                                // prepare
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));

    bool out{};
    // s1 is highest priority
    ASSERT_EQ(Status::OK, check_ltx_is_highest_priority(s1, out));
    ASSERT_TRUE(out);

    // s2 isn't highest priority
    ASSERT_EQ(Status::OK, check_ltx_is_highest_priority(s2, out));
    ASSERT_FALSE(out);

    wait_epoch_update();
    // s1 finish
    ASSERT_EQ(Status::OK, commit(s1));

    // s1 is not bgan
    ASSERT_EQ(Status::WARN_NOT_BEGIN, check_ltx_is_highest_priority(s1, out));

    // s2 is highest priority
    ASSERT_EQ(Status::OK, check_ltx_is_highest_priority(s2, out));
    ASSERT_TRUE(out);

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
