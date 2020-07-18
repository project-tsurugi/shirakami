#include "gtest/gtest.h"

#include "error.hh"

namespace shirakami::testing {

class errorTest : public ::testing::Test { // NOLINT
};

TEST_F(errorTest, LibcError) { // NOLINT
  std::string errorstring{};
  try {
    throw LibcError(errno, errorstring);
  } catch (...) {
    std::cout << "catch block." << std::endl;
    std::cout << errorstring << std::endl;
  }
}

}  // namespace shirakami::testing
