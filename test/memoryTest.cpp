#include "gtest/gtest.h"
#include "memory.h"

namespace shirakami::testing {

class memoryTest : public ::testing::Test {};

TEST_F(memoryTest, displayRusageRUMaxrss) {  // NOLINT
  displayRusageRUMaxrss();
}

}  // namespace shirakami::testing
