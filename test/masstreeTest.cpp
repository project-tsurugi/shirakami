/**
 * @file masstreeTest.cpp
 * @brief test about usage of masstree.
 */

#include "compiler.hh"
#include "gtest/gtest.h"
#include "header.hh"
#include "kvs/interface.h"
#include "masstree_wrapper.hh"
#include "scheme.hh"
#include "xact.hh"

// to use declaration of entity of global variables.
#include "./../src/masstree_wrapper.cc"

using namespace kvs;
using namespace std;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class MasstreeTest : public ::testing::Test {
 protected:
  MasstreeTest() {}
  ~MasstreeTest() {}
};

TEST_F(MasstreeTest, insert_third) {
  MasstreeWrapper<uint64_t> MT;
  uint64_t key = {0};
  ASSERT_EQ(Status::OK, MT.insert_value((char*)(&key), sizeof(&key), &key));
  ASSERT_EQ(Status::WARN_ALREADY_EXISTS,
            MT.insert_value((char*)(&key), sizeof(&key), &key));
  ASSERT_EQ(Status::WARN_ALREADY_EXISTS,
            MT.insert_value((char*)(&key), sizeof(&key), &key));
}

TEST_F(MasstreeTest, remove) {
  MasstreeWrapper<uint64_t> MT;
  uint64_t key = {0};
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            MT.remove_value(reinterpret_cast<char*>(&key), sizeof(key)));
  ASSERT_EQ(Status::OK,
            MT.insert_value(reinterpret_cast<char*>(&key), sizeof(&key), &key));
  ASSERT_EQ(Status::OK,
            MT.remove_value(reinterpret_cast<char*>(&key), sizeof(&key)));
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            MT.remove_value(reinterpret_cast<char*>(&key), sizeof(key)));
}

}  // namespace kvs_charkey::testing
