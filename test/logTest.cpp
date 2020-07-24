/**
 * @file logTest.cpp
 */

#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/log.h"
#endif

#include "gtest/gtest.h"

namespace shirakami::testing {

class logTest : public ::testing::Test {};  // NOLINT

TEST_F(logTest, LogHeader) {  // NOLINT
  shirakami::Log::LogHeader lh;
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

TEST_F(logTest, LogRecord) {  // NOLINT
  using namespace shirakami;
  Log::LogRecord lr{};
  ASSERT_EQ(lr.get_tid().get_obj(), 0);
  ASSERT_EQ(lr.get_op(), OP_TYPE::NONE);
  ASSERT_EQ(lr.get_tuple(), nullptr);
}

}  // namespace shirakami::testing
