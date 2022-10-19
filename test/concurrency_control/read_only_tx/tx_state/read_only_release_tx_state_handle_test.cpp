
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_release_tx_state_handle_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "read_only_release_tx_state_handle_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(read_only_release_tx_state_handle_test, read_only_call_twice) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
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
