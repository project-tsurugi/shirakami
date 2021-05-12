#include "shirakami/interface.h"

#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "compiler.h"
#include "concurrency_control/include/scheme.h"
#include "tuple_local.h"

namespace shirakami::testing {

using namespace shirakami;

class helper : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/test/helper_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(helper, project_root) { // NOLINT
    /**
     * MAC2STR macro is used at init function.
     */
    std::cout << MAC2STR(PROJECT_ROOT) << std::endl;
    std::string str(MAC2STR(PROJECT_ROOT)); // NOLINT
    str.append("/log/");
    std::cout << str << std::endl;
}

TEST_F(helper, enter) { // NOLINT
    std::array<Token, 2> s{nullptr, nullptr};
    ASSERT_EQ(Status::OK, enter(s[0]));
    ASSERT_EQ(Status::OK, enter(s[1]));
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

} // namespace shirakami::testing
