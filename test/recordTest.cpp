#include "gtest/gtest.h"

#include "record.hh"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class recordTest : public ::testing::Test {
protected:
  recordTest() {}
  ~recordTest() {}
};

TEST_F(recordTest, constructor) {
  {
    Record rec;
    ASSERT_EQ(rec.get_tidw().get_obj(), 0);
    Tuple& tuple = rec.get_tuple();
    ASSERT_EQ(tuple.get_key().size(), 0);
    ASSERT_EQ(tuple.get_value().size(), 0);
  }
}

}  // namespace kvs_charkey::testing
