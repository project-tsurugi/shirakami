
#include "gtest/gtest.h"

namespace shirakami::testing {

class string_view_test : public ::testing::Test {
};

TEST_F(string_view_test, compare) {  // NOLINT
    std::string_view sv{""};
    ASSERT_EQ(sv.empty(), true);
    //ASSERT_EQ(sv.data() == nullptr, true); // it will fail
}

}  // namespace shirakami::testing
