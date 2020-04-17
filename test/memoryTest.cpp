#include "gtest/gtest.h"

#include "memory.hh"

using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class memoryTest : public ::testing::Test {
protected:
  memoryTest() {}
  ~memoryTest() {}
};

TEST_F(memoryTest, displayRusageRUMaxrss) { displayRusageRUMaxrss(); }

}  // namespace kvs_charkey::testing
