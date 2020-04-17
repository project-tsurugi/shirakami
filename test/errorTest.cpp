#include "gtest/gtest.h"

#include "error.hh"

using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class errorTest : public ::testing::Test {
protected:
  errorTest() {}
  ~errorTest() {}
};

TEST_F(errorTest, LibcError) {
  std::string errorstring;
  try {
    throw LibcError(errno, errorstring);
  } catch (...) {
    cout << "catch block." << endl;
    cout << errorstring << endl;
  }
}

}  // namespace kvs_charkey::testing
