
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_tx_begin_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "helper-long_tx_begin_test");
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

TEST_F(long_tx_begin_test, tx_begin_wp) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::vector<Storage> wp{1, 2, 3};
    // wp for non-existing storage
    ASSERT_EQ(Status::ERR_FAIL_WP, tx_begin(s, TX_TYPE::LONG, wp));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing