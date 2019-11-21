
#include "gtest/gtest.h"

#include "kvs/interface.h"

#include "include/header.hh"
#include "include/scheme.h"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class SimpleTest : public ::testing::Test {
protected:
  SimpleTest() {
    kvs::init();
  }
};

TEST_F(SimpleTest, enter) {
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::WARN_ALREADY_IN_A_SESSION, enter(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, leave) {
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
}

TEST_F(SimpleTest, insert) {
  std::string k("aaa");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, scan) {
  std::string k("aaa");
  std::string k2("aab");
  std::string k3("aac");
  std::string k4("aad");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<Tuple*> records{};
  cout << "SimpleTest : typeinfo : typeid(Record).name() : "
    << typeid(Record).name() << endl;
  cout << "SimpleTest : typeinfo : typeid(Record*).name() : "
    << typeid(Record*).name() << endl;
  cout << "SimpleTest : start : scan "
    << k.data() << " l_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k4.data(), k4.size(), false, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  cout << "SimpleTest : start : scan "
    << k.data() << " l_exclusive == true" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(), k4.size(), false, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  cout << "SimpleTest : start : scan "
    << k.data() << " - " << k3.data()
    << " l_exclusive == false, "
    << "r_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k3.data(), k3.size(), false, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  cout << "SimpleTest : start : scan "
    << k.data() << " - " << k3.data()
    << " l_exclusive == false, "
    << "r_exclusive == true" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k3.data(), k3.size(), true, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  delete_record(s, st, k.data(), k.size());
  delete_record(s, st, k2.data(), k2.size());
  delete_record(s, st, k3.data(), k3.size());
  commit(s);
  leave(s);
}
}
