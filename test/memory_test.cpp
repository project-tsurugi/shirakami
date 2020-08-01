#include "gtest/gtest.h"
#include "memory.h"

namespace shirakami::testing {

class memory : public ::testing::Test {};

TEST_F(memory, displayRusageRUMaxrss) {  // NOLINT
  displayRusageRUMaxrss();
}

}  // namespace shirakami::testing
