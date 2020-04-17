#include "gtest/gtest.h"

#include "log.hh"

using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class logTest : public ::testing::Test {
protected:
  logTest() {}
  ~logTest() {}
};

TEST_F(logTest, LogHeader) {
  LogHeader lh;
  lh.init();
  ASSERT_EQ(lh.get_checksum(), 0);
  ASSERT_EQ(lh.get_log_rec_num(), 0);
  lh.set_checksum(1);
  ASSERT_EQ(lh.get_checksum(), 1);
  lh.add_checksum(1);
  ASSERT_EQ(lh.get_checksum(), 2);
  lh.inc_log_rec_num();
  ASSERT_EQ(lh.get_log_rec_num(), 1);
}

}  // namespace kvs_charkey::testing
