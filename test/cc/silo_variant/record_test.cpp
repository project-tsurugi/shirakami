#include "cc/silo_variant/include/record.h"

#include "tuple_local.h"

#include "gtest/gtest.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

class record : public ::testing::Test {};

TEST_F(record, constructor) {  // NOLINT
  {
    Record rec{};
    ASSERT_EQ(rec.get_tidw().get_obj(), 0);
    Tuple& tuple = rec.get_tuple();
    ASSERT_EQ(tuple.get_key().size(), 0);
    ASSERT_EQ(tuple.get_value().size(), 0);
  }
}

}  // namespace shirakami::testing