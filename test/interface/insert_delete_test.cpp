#include "kvs/interface.h"

#include <bitset>

#include "gtest/gtest.h"

#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class insert_delete : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override { fin(); }
};

TEST_F(insert_delete, insert_delete_with_16chars) {  // NOLINT
  std::string k("testing_a0123456");                  // NOLINT
  std::string v("bbb");                               // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto&& t : records) {
    ASSERT_EQ(Status::OK,
              delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, insert_delete_with_10chars) {  // NOLINT
  std::string k("testing_a0");                        // NOLINT
  std::string v("bbb");                               // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto&& t : records) {
    ASSERT_EQ(Status::OK,
              delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
