
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_release_tx_state_handle_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "long_release_tx_state_handle_test");
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

TEST_F(long_release_tx_state_handle_test, long_call_twice) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG}));
    TxStateHandle hd{};
    LOG(INFO);
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s, hd));
    LOG(INFO);
    ASSERT_NE(undefined_handle, hd);
    LOG(INFO);
    ASSERT_EQ(Status::OK, release_tx_state_handle(hd));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, release_tx_state_handle(hd));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
