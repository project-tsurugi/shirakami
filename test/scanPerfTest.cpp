
#include "gtest/gtest.h"

#include "kvs/interface.h"

#include "include/compiler.hh"
#include "include/header.hh"
#include "include/scheme.hh"
#include "include/tsc.hh"
#include "include/xact.hh"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class ScanPerfTest : public ::testing::Test {
 protected:
  ScanPerfTest() { kvs::init(); }
  ~ScanPerfTest() { 
    kvs::fin(); 
    kvs::delete_all_records();
    kvs::delete_all_garbage_records();
    //kvs::MTDB.destroy();
  }
};

TEST_F(ScanPerfTest, read_from_scan) {
  uint64_t key[10000];
  for (int i = 0; i < 10000; ++i) {
    key[i] = i;
  }
  std::string v1("a");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  uint64_t start, end;

  tbegin(s);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 100 records." << endl;
  ScanHandle handle{};
  Tuple* tuple{};
  start = rdtscp();
  tbegin(s);
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure only 100 records index.
   * We do a simple check because we can't know how many the index was scanned from outside of kvs.
   */
  ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  tbegin(s);
  for (int i = 100; i < 10000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 10000 records." << endl;
  start = rdtscp();
  tbegin(s);
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure that there are many records index more than 100 records index.
   * We do a simple check because we can't know how many the index was scanned from outside of kvs.
   */
  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;
}

} // namespace kvs_charkey::testing
