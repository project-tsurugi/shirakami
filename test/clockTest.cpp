#include "gtest/gtest.h"

#include "clock.hh"

using std::cout;
using std::endl;

namespace shirakami::testing {

class clockTest : public ::testing::Test {
protected:
  clockTest() {}
  ~clockTest() {}
};

TEST_F(clockTest, check_clock_span) {
  uint64_t start, stop, threshold(UINT64_MAX);
  start = 1;
  stop = 2;
  ASSERT_EQ(false, check_clock_span(start, stop, threshold));
  start = 1;
  stop = UINT64_MAX;
  threshold = 10;
  ASSERT_EQ(true, check_clock_span(start, stop, threshold));
}

TEST_F(clockTest, sleepMs) {
  sleepMs(1);
  // this assert means that it confirms that the above line ends normally.
  ASSERT_EQ(true, true);
}

}  // namespace shirakami::testing
