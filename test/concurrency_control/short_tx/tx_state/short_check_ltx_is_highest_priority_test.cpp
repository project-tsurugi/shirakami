
#include "test_tool.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_check_ltx_is_highest_priority_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "tx_state-short_check_ltx_is_highest_priority_test");
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

TEST_F(short_check_ltx_is_highest_priority_test, // NOLINT
       simple) {                                 // NOLINT
                                                 // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));


    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    // short mode is invalid for this api
    bool out{};
    ASSERT_EQ(Status::WARN_INVALID_ARGS, check_ltx_is_highest_priority(s, out));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing