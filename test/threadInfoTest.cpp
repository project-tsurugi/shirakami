
#include "gtest/gtest.h"

#include "kvs/interface.h"

// kvs_charkey-impl interface library
#include "compiler.hh"
#include "header.hh"
#include "scheme.hh"
#include "xact.hh"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class ThreadInfoTest : public ::testing::Test {
 protected:
  ThreadInfoTest() { kvs::init(); }
  ~ThreadInfoTest() { 
    kvs::fin(); 
    kvs::delete_all_records();
    kvs::delete_all_garbage_records();
    kvs::delete_all_garbage_values();
    //kvs::MTDB.destroy();
  }
};

TEST_F(ThreadInfoTest, get_txbegan_) {
  std::string k("aaa");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ThreadInfo* ti = static_cast<ThreadInfo*>(s);
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
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(), k.size(), false, res));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(), k.size(), false, res));
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  // test open_scan
  ScanHandle handle = {};
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k.data(), k.size(), false, handle));
  ASSERT_EQ(handle, 0);
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(ti->get_txbegan(), false);
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k.data(), k.size(), false, handle));
  ASSERT_EQ(handle, 0);
  ASSERT_EQ(ti->get_txbegan(), true);
  ASSERT_EQ(Status::OK, commit(s));
}

}  // namespace kvs_charkey::testing
