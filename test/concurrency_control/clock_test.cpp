#include "clock.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

class clock_test : public ::testing::Test {  // NOLINT
};

TEST_F(clock_test, check_clock_span) {  // NOLINT
    uint64_t start{1};
    uint64_t stop{2};
    uint64_t threshold{UINT64_MAX};
    ASSERT_EQ(false, check_clock_span(start, stop, threshold));
    start = 1;
    stop = UINT64_MAX;
    threshold = 10;  // NOLINT
    ASSERT_EQ(true, check_clock_span(start, stop, threshold));
}

TEST_F(clock_test, sleepMs) {  // NOLINT
    sleepMs(1);
    // this assert means that it confirms that the above line ends normally.
    ASSERT_EQ(true, true);
}

}  // namespace shirakami::testing