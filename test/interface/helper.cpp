#include "kvs/interface.h"

#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "cc/silo_variant/include/scheme.h"
#include "compiler.h"
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class helper : public ::testing::Test {  // NOLINT
public:
    void SetUp() override { init(); }  // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(helper, project_root) {  // NOLINT
    /**
     * MAC2STR macro is used at init function.
     */
    std::cout << MAC2STR(PROJECT_ROOT) << std::endl;
    std::string str(MAC2STR(PROJECT_ROOT));  // NOLINT
    str.append("/log/");
    std::cout << str << std::endl;
}

TEST_F(helper, enter) {  // NOLINT
    std::array<Token, 2> s{nullptr, nullptr};
    ASSERT_EQ(Status::OK, enter(s[0]));
    ASSERT_EQ(Status::OK, enter(s[1]));
    ASSERT_EQ(Status::OK, leave(s[0]));
    ASSERT_EQ(Status::OK, leave(s[1]));
}

TEST_F(helper, leave) {  // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
    ASSERT_EQ(Status::ERR_INVALID_ARGS, leave(nullptr));
}

}  // namespace shirakami::testing
