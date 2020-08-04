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

class scan_search : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override { fin(); }
};

TEST_F(scan_search, scan_key_search_key) {  // NOLINT
  std::string k("a");                       // NOLINT
  std::string k2("aa");                     // NOLINT
  std::string k3("aac");                    // NOLINT
  std::string k4("b");                      // NOLINT
  std::string v("bbb");                     // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, "", false, "", false, records));
  ASSERT_EQ(Status::OK, commit(s));
  for (auto&& itr : records) {
    std::cout << std::string(itr->get_key().data(),  // NOLINT
                             itr->get_key().size())
              << std::endl;
  }
  records.clear();
  ASSERT_EQ(Status::OK, upsert(s, st, k, v));
  ASSERT_EQ(Status::OK, upsert(s, st, k2, v));
  ASSERT_EQ(Status::OK, upsert(s, st, k3, v));
  ASSERT_EQ(Status::OK, upsert(s, st, k4, v));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k, true, k4, true, records));
  EXPECT_EQ(2, records.size());

  Tuple* tuple{};
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            search_key(s, st, k2, &tuple));
  EXPECT_NE(nullptr, tuple);
  delete_record(s, st, k2);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k, true, k4, true, records));
  EXPECT_EQ(1, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(scan_search, mixing_scan_and_search) {  // NOLINT
  std::string k1("aaa");                       // NOLINT
  std::string k2("aab");                       // NOLINT
  std::string k3("xxx");                       // NOLINT
  std::string k4("zzz");                       // NOLINT
  std::string v1("bbb");                       // NOLINT
  std::string v2("bbb");                       // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k1, v1));
  ASSERT_EQ(Status::OK, insert(s, st, k2, v2));
  ASSERT_EQ(Status::OK, insert(s, st, k4, v2));
  ASSERT_EQ(Status::OK, commit(s));
  ScanHandle handle{};
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, open_scan(s, st, k1, false, k2, false, handle));
  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(memcmp(tuple->get_key().data(), k1.data(), k1.size()), 0);
  ASSERT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);

  // record exists
  ASSERT_EQ(Status::OK, search_key(s, st, k4, &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);

  // record not exist
  ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, k3, &tuple));

  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(memcmp(tuple->get_key().data(), k2.data(), k2.size()), 0);
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, commit(s));

  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
