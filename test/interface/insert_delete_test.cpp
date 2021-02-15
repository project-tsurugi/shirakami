#include <bitset>

#include "gtest/gtest.h"
#include "kvs/interface.h"
#include "tuple_local.h"

using namespace shirakami::cc_silo_variant;

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class insert_delete : public ::testing::Test {  // NOLINT
public:
    void SetUp() override { init(); }  // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(insert_delete, insert_delete_with_16chars) {  // NOLINT
    std::string k("testing_a0123456");                 // NOLINT
    std::string v("bbb");                              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> tuples{};
    ASSERT_EQ(Status::OK, scan_key(s, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, tuples));
    EXPECT_EQ(1, tuples.size());
    for (auto &&t : tuples) {
        ASSERT_EQ(Status::OK, delete_record(s, t->get_key()));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, insert_delete_with_10chars) {  // NOLINT
    std::string k("testing_a0");                       // NOLINT
    std::string v("bbb");                              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, records));
    EXPECT_EQ(1, records.size());
    for (auto &&t : records) {
        ASSERT_EQ(Status::OK, delete_record(s, t->get_key()));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, delete_insert) {  // NOLINT
    std::string k("testing");                       // NOLINT
    std::string v("bbb");                              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* t{};
    ASSERT_EQ(Status::OK, search_key(s, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, delete_insert_on_scan) {  // NOLINT
  std::string k("testing");                       // NOLINT
  std::string k2("new_key");                       // NOLINT
  std::string v("bbb");                              // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, insert(s, k, v));
  ASSERT_EQ(Status::OK, commit(s)); // NOLINT
  Tuple* t{};
  ScanHandle handle;
  ASSERT_EQ(Status::OK, open_scan(
      s,
      k, scan_endpoint::INCLUSIVE,
      "", scan_endpoint::INF,
      handle)
  );
  ASSERT_EQ(Status::OK, read_from_scan(
      s,
      handle,
      &t)
  );
  ASSERT_TRUE(t); // NOLINT
  ASSERT_EQ(Status::OK, delete_record(s, k));
  ASSERT_EQ(Status::OK, insert(s, k2, v));
  ASSERT_EQ(Status::OK, close_scan(s, handle));
  ASSERT_EQ(Status::OK, commit(s)); // NOLINT
  ASSERT_EQ(Status::OK, search_key(s, k2, &t));
  ASSERT_TRUE(t); // NOLINT
  ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, k, &t));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, delete_upsert) {  // NOLINT
    std::string k("testing");                       // NOLINT
    std::string v("bbb");                              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* t{};
    ASSERT_EQ(Status::OK, search_key(s, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
