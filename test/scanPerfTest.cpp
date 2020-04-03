
#include "gtest/gtest.h"

#include "kvs/interface.h"

#include "include/compiler.hh"
#include "include/header.hh"
#include "include/tsc.hh"

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
  }
};

TEST_F(ScanPerfTest, read_from_scan) {
  uint64_t key[1000000];
  for (int i = 0; i < 1000000; ++i) {
    key[i] = i;
  }
  std::string v1("a");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  uint64_t start, end, scan_size;

  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 100 records." << endl;
  ScanHandle handle{};
  Tuple* tuple{};
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 100; i < 1000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 1K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 1000; i < 10000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 10K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 10000; i < 20000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 20K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 20000; i < 40000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 40K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 40000; i < 80000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 80K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 80000; i < 160000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 160K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  /**
   * Prepare for next experiments.
   */
  for (int i = 160000; i < 320000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 320K records." << endl;
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;

  ASSERT_EQ(Status::OK, leave(s));
}

} // namespace kvs_charkey::testing
