/**
 * @file recordTest.cpp
 */

#include "cc/silo_variant/include/record.h"

#include "tuple_local.h"

#include "gtest/gtest.h"

using namespace shirakami::silo_variant;

namespace shirakami::testing {

class recordTest : public ::testing::Test {};

TEST_F(recordTest, constructor) {  // NOLINT
  {
    Record rec{};
    ASSERT_EQ(rec.get_tidw().get_obj(), 0);
    Tuple& tuple = rec.get_tuple();
    ASSERT_EQ(tuple.get_key().size(), 0);
    ASSERT_EQ(tuple.get_value().size(), 0);
  }
}

}  // namespace shirakami::testing
