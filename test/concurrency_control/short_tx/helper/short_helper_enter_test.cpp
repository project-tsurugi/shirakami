
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class c_helper_enter : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "helper-c_helper_enter_test");
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

TEST_F(c_helper_enter, enter) { // NOLINT
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

} // namespace shirakami::testing
