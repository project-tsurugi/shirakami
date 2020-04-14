
#include "gtest/gtest.h"

#include "kvs/interface.h"

// kvs_charkey-impl interface library
#include "compiler.hh"
#include "header.hh"
#include "tsc.hh"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

constexpr const int MAX_TUPLES = 1000000;
constexpr const int READ_TUPLES = 100;

class ScanPerfTest : public ::testing::Test {
 protected:
  ScanPerfTest() {
    kvs::init();
    for (int i = 0; i < MAX_TUPLES; ++i) {
      key_[i] = i;
    }
  }
  ~ScanPerfTest() { 
    kvs::fin(); 
    kvs::delete_all_records();
    kvs::delete_all_garbage_records();
  }

  void DoScan();
  void DoInsert(int, int);

  uint64_t key_[MAX_TUPLES];
  Token s_{};
  Storage st_{};
};

void
ScanPerfTest::DoInsert(int bgn_idx, int end_idx)
{
  std::string v1("a");

  for (int i = bgn_idx; i < end_idx; ++i) {
    EXPECT_EQ(Status::OK, insert(s_, st_, (char *)&key_[i], sizeof(key_[i]), v1.data(), v1.size()));
  }
  EXPECT_EQ(Status::OK, commit(s_));
}

void
ScanPerfTest::DoScan()
{
  uint64_t start, end, scan_size;
  ScanHandle handle{};
  Tuple* tuple{};

  start = rdtscp();
  EXPECT_EQ(Status::OK, open_scan(s_, st_, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < READ_TUPLES; ++i) {
    EXPECT_EQ(Status::OK, read_from_scan(s_, st_, handle, &tuple));
  }
  /**
   * Make sure the scan size.
   */
  EXPECT_EQ(Status::OK, scannable_total_index_size(s_, st_, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  EXPECT_EQ(Status::OK, commit(s_));
  end = rdtscp();
  cout << "Result : " << end - start << " [clocks]" << endl;
}

TEST_F(ScanPerfTest, read_from_scan) {
  EXPECT_EQ(Status::OK, enter(s_));

  DoInsert(0, 100);
  cout << "Perform 100 records read_from_scan on a table with 100 records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(100, 1000);
  cout << "Perform 100 records read_from_scan on a table with 1K records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(1000, 10000);
  cout << "Perform 100 records read_from_scan on a table with 10K records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(10000, 20000);
  cout << "Perform 100 records read_from_scan on a table with 20K records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(20000, 40000);
  cout << "Perform 100 records read_from_scan on a table with 40K records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(40000, 80000);
  cout << "Perform 100 records read_from_scan on a table with 80K records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(80000, 160000);
  cout << "Perform 100 records read_from_scan on a table with 160K records." << endl;
  DoScan();

  /**
   * Prepare for next experiments.
   */
  DoInsert(160000, 320000);
  cout << "Perform 100 records read_from_scan on a table with 320K records." << endl;
  DoScan();

  EXPECT_EQ(Status::OK, leave(s_));
}

}  // namespace kvs_charkey::testing
