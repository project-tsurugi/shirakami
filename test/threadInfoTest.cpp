/**
 * @file threadInfoTest.cpp
 */

#include "gtest/gtest.h"

#include "kvs/interface.h"

#include "cc/silo_variant/include/thread_info.h"

#include "tuple_local.h"

using namespace shirakami;

namespace shirakami::testing {

class ThreadInfoTest : public ::testing::Test {
public:
  void SetUp() override { shirakami::init(); }  // NOLINT

  void TearDown() override { shirakami::fin(); }
};

TEST_F(ThreadInfoTest, get_txbegan_) {  // NOLINT
  std::string k("aaa");                 // NOLINT
  std::string v("bbb");                 // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  auto* ti = static_cast<ThreadInfo*>(s);
  ASSERT_EQ(ti->get_txbegan(), false);
  // test upsert
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test delete
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test insert
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test update
  ASSERT_EQ(Status::OK, update(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, update(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test search
  Tuple* tuple = {};
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test scan
  std::vector<const Tuple*> res;
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, res));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, res));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test open_scan
  ScanHandle handle = {};
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k.data(),
                                  k.size(), false, handle));
  ASSERT_EQ(handle, 0);
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k.data(),
                                  k.size(), false, handle));
  ASSERT_EQ(handle, 0);
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));

  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
