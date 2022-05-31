
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class helper : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-wp_test");
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

TEST_F(helper, tx_begin_wp) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::vector<Storage> wp{1, 2, 3};
    // wp for non-existing storage
    ASSERT_EQ(Status::ERR_FAIL_WP, tx_begin(s, false, true, wp));
    ASSERT_EQ(Status::OK, tx_begin(s));
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_begin(s));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(helper, tx_begin_read_only_and_wp) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, tx_begin(s, true, true, {st}));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
