#include "gtest/gtest.h"
#include "memory.h"

namespace shirakami::testing {

class memory_test : public ::testing::Test {};

TEST_F(memory_test, displayRusageRUMaxrss) {  // NOLINT
  displayRusageRUMaxrss();
}

}  // namespace shirakami::testing
