/**
 * @file masstreeTest.cpp
 * @brief test about usage of masstree.
 */

#include "cc/silo_variant/include/scheme_local.h"
#include "include/tuple_local.h"
#include "gtest/gtest.h"
#include "index/masstree_beta/include/masstree_beta_wrapper.h"

#ifdef INDEX_KOHLER_MASSTREE
// to use declaration of entity of global variables.
#include "index/masstree_beta/masstree_beta_wrapper.cpp"
#endif

using namespace shirakami;

namespace shirakami::testing {

class MasstreeTest : public ::testing::Test {};  // NOLINT

TEST_F(MasstreeTest, insert_third) {  // NOLINT
  MasstreeWrapper<std::uint64_t> MT;
  std::uint64_t key{0};
  ASSERT_EQ(Status::OK,
            MT.insert_value(reinterpret_cast<char*>(&key),  // NOLINT
                            sizeof(&key), &key));
  ASSERT_EQ(
      Status::WARN_ALREADY_EXISTS,
      MT.insert_value(reinterpret_cast<char*>(&key), sizeof(&key),  // NOLINT
                      &key));
  ASSERT_EQ(
      Status::WARN_ALREADY_EXISTS,
      MT.insert_value(reinterpret_cast<char*>(&key), sizeof(&key),  // NOLINT
                      &key));
}

TEST_F(MasstreeTest, remove) {  // NOLINT
  MasstreeWrapper<std::uint64_t> MT;
  std::uint64_t key{0};
  ASSERT_EQ(
      Status::WARN_NOT_FOUND,
      MT.remove_value(reinterpret_cast<char*>(&key), sizeof(key)));  // NOLINT
  ASSERT_EQ(Status::OK,
            MT.insert_value(reinterpret_cast<char*>(&key),  // NOLINT
                            sizeof(&key), &key));
  ASSERT_EQ(Status::OK,
            MT.remove_value(reinterpret_cast<char*>(&key),  // NOLINT
                            sizeof(&key)));
  ASSERT_EQ(
      Status::WARN_NOT_FOUND,
      MT.remove_value(reinterpret_cast<char*>(&key), sizeof(key)));  // NOLINT
}

}  // namespace shirakami::testing
