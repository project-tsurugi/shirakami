#include "error.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

class error_test : public ::testing::Test {  // NOLINT
};

TEST_F(error_test, LibcError) {  // NOLINT
  std::string errorstring{};
  try {
    throw LibcError(errno, errorstring);
  } catch (...) {
    std::cout << "catch block." << std::endl;
    std::cout << errorstring << std::endl;
  }
}

}  // namespace shirakami::testing
