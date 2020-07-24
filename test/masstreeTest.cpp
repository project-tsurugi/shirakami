/**
 * @file masstreeTest.cpp
 * @brief test about usage of masstree.
 */

#include "gtest/gtest.h"
#include "masstree_beta_wrapper.h"
#include "scheme_local.h"
#include "tuple_local.h"

// to use declaration of entity of global variables.
#include "./../src/masstree_beta_wrapper.cpp"

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
