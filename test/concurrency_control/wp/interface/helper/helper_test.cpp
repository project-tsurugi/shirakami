
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
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/helper_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(helper, init) { // NOLINT
    ASSERT_EQ(init(), Status::WARN_ALREADY_INIT);
    fin();
    ASSERT_EQ(init(), Status::OK);
}

TEST_F(helper, fin) { // NOLINT
    // meaningless fin
    fin();
    fin();
}

TEST_F(helper, enter) { // NOLINT
    std::array<Token, 2> s{nullptr, nullptr};
    ASSERT_EQ(s.at(0), s.at(1));
    ASSERT_EQ(s.at(0), nullptr);
    ASSERT_EQ(Status::OK, enter(s[0]));
    ASSERT_NE(s.at(0), nullptr);
    ASSERT_EQ(s.at(1), nullptr);
    ASSERT_EQ(Status::OK, enter(s[1]));
    ASSERT_NE(s.at(1), nullptr);
    ASSERT_NE(s.at(0), s.at(1));
    ASSERT_EQ(Status::OK, leave(s[0]));
    ASSERT_EQ(Status::OK, leave(s[1]));
}

TEST_F(helper, leave) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
    ASSERT_EQ(Status::WARN_INVALID_ARGS, leave(nullptr));
}

TEST_F(helper, tx_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s));
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_begin(s));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin(s));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<Storage> wp{1, 2, 3};
    // wp for non-existing storage
    ASSERT_EQ(Status::ERR_FAIL_WP, tx_begin(s, false, true, wp));
    ASSERT_EQ(Status::OK, tx_begin(s));
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_begin(s));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing