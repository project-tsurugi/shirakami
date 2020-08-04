#include <bitset>

#include "gtest/gtest.h"
#include "kvs/interface.h"

// shirakami-impl interface library
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class simple_insert : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override { fin(); }
};

TEST_F(simple_insert, insert) {  // NOLINT
  std::string k("aaa");          // NOLINT
  std::string v("bbb");          // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k, v));
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(Status::OK, insert(s, st, k, v));
  ASSERT_EQ(Status::OK, commit(s));
  {
    Tuple* tuple{};
    char k2 = 0;
    ASSERT_EQ(Status::OK, insert(s, st, {&k2, 1}, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, search_key(s, st, {&k2, 1}, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
    ASSERT_EQ(Status::OK, commit(s));
  }
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, insert(s, st, "", v));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, "", &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_insert, long_insert) {  // NOLINT
  std::string k("CUSTOMER");          // NOLINT
  std::string v(                      // NOLINT
      "b234567890123456789012345678901234567890123456789012345678901234567890"
      "12"
      "3456789012345678901234567890123456789012345678901234567890123456789012"
      "34"
      "5678901234567890123456789012345678901234567890123456789012345678901234"
      "56"
      "7890123456789012345678901234567890123456789012345678901234567890123456"
      "78"
      "9012345678901234567890123456789012345678901234567890123456789012345678"
      "90"
      "1234567890123456789012345678901234567890123456789012345678901234567890"
      "12"
      "3456789012345678901234567890123456789012345678901234567890123456789012"
      "34"
      "5678901234567890123456789012345678901234567890123456789012345678901234"
      "56"
      "7890123456789012345678901234567890123456789012345678901234567890123456"
      "78"
      "9012345678901234567890123456789012345678901234567890123456789012345678"
      "90"
      "1234567890123456789012345678901234567890123456789012345678901234567890"
      "12"
      "3456789012345678901234567890123456789012345678901234567890123456789012"
      "34"
      "5678901234567890123456789012345678901234567890");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k, v));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
