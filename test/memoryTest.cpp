#include "gtest/gtest.h"

#include "memory.hh"

using std::cout;
using std::endl;

namespace shirakami::testing {

class memoryTest : public ::testing::Test {
protected:
  memoryTest() {}
  ~memoryTest() {}
};

TEST_F(memoryTest, displayRusageRUMaxrss) { displayRusageRUMaxrss(); }

}  // namespace shirakami::testing
