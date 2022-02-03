

#include "concurrency_control/silo/include/record.h"
#include "concurrency_control/silo/include/tuple_local.h"

#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class record : public ::testing::Test {};

TEST_F(record, constructor) { // NOLINT
    {
        Record rec{};
        ASSERT_EQ(rec.get_tidw().get_obj(), 0);
        Tuple& tuple = rec.get_tuple();
        std::string key{};
        tuple.get_key(key);
        ASSERT_EQ(key.size(), 0);
        std::string val{};
        tuple.get_value(val);
        ASSERT_EQ(val.size(), 0);
    }
}

} // namespace shirakami::testing
