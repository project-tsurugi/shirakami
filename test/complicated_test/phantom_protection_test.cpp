#include "gtest/gtest.h"
#include "kvs/interface.h"

namespace shirakami::testing {

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

class phantom_protection : public ::testing::Test {
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override { fin(); }
};

TEST_F(phantom_protection, phantom) {  // NOLINT
  constexpr std::size_t token_length{2};
  std::array<Token, token_length> token{};
  ASSERT_EQ(enter(token.at(0)), Status::OK);
  ASSERT_EQ(enter(token.at(1)), Status::OK);
  constexpr std::size_t key_length{3};
  std::array<std::string, 3> key;
  for (std::size_t i = 0; i < key_length; ++i) {
    key.at(i) = std::string{1, static_cast<char>(i)};  // NOLINT
  }
  std::string v{"value"};  // NOLINT
  Storage st{};
  ASSERT_EQ(Status::OK, insert(token.at(0), st, key.at(0), v));
  ASSERT_EQ(Status::OK, insert(token.at(0), st, key.at(1), v));
  ASSERT_EQ(Status::OK, commit(token.at(0)));
  std::vector<const Tuple*> tuple_vec;
  ASSERT_EQ(Status::OK,
            scan_key(token.at(0), st, "", false, "", false, tuple_vec));
  ASSERT_EQ(tuple_vec.size(), 2);
  // interrupt to occur phantom
  ASSERT_EQ(Status::OK, insert(token.at(1), st, key.at(2), v));
  ASSERT_EQ(Status::OK, commit(token.at(1)));
  // =====
  ASSERT_EQ(Status::ERR_VALIDATION, commit(token.at(0)));
  // abort by phantom
  ASSERT_EQ(leave(token.at(0)), Status::OK);
  ASSERT_EQ(leave(token.at(1)), Status::OK);
}

}  // namespace shirakami::testing
