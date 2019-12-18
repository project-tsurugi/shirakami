
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
  Token s[] = {nullptr, nullptr};
  ASSERT_EQ(Status::OK, enter(s[0]));
  ASSERT_EQ(Status::OK, enter(s[1]));
  ASSERT_EQ(Status::OK, leave(s[0]));
  ASSERT_EQ(Status::OK, leave(s[1]));
}

TEST_F(SimpleTest, leave) {
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
  ASSERT_EQ(Status::ERR_INVALID_ARGS, leave(nullptr));
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
  {
    Tuple *tuple;
    char k2 = 0;
    ASSERT_EQ(Status::OK, insert(s, st, &k2, 1, v.data(), v.size()));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, search_key(s, st, &k2, 1, &tuple));
    ASSERT_EQ(memcmp(tuple->val.get(), v.data(), 3), 0); 
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, delete_record(s, st, &k2, 1));
    ASSERT_EQ(Status::OK, commit(s));
  }
  Tuple *tuple;
  ASSERT_EQ(Status::OK, insert(s, st, nullptr, 0, v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, nullptr, 0, &tuple));
  ASSERT_EQ(memcmp(tuple->val.get(), v.data(), 3), 0); 
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, nullptr, 0));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, update) {
  std::string k("aaa");
  std::string v("aaa");
  std::string v2("bbb");
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::ERR_NOT_FOUND, update(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple *tuple;
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  cout << "SimpleTest : update : "
    << std::string(tuple->val.get(), tuple->len_val) << endl;
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, update(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  cout << "SimpleTest : update : "
    << std::string(tuple->val.get(), tuple->len_val) << endl;
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, search) {
  std::string k("aaa");
  std::string v("bbb");
  Token s{};
  Storage st{};
  Tuple *tuple;
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::ERR_NOT_FOUND, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, upsert) {
  std::string k("aaa");
  std::string v("aaa");
  std::string v2("bbb");
  Token s{};
  Storage st{};
  Tuple *tuple;
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::ERR_ALREADY_EXISTS, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  cout << "SimpleTest : upsert : "
    << std::string(tuple->val.get(), tuple->len_val) << endl;
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, delete_) {
  std::string k("aaa");
  std::string v("aaa");
  std::string v2("bbb");
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::ERR_NOT_FOUND, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::ERR_NOT_FOUND, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, scan) {
  std::string k("aaa");
  std::string k2("aab");
  std::string k3("aac");
  std::string k4("aad");
  std::string k5("aadd");
  std::string k6("aa");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  cout << static_cast<int>(insert(s, st, nullptr, 0, v.data(), v.size())) << endl;
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k6.data(), k6.size(), v.data(), v.size()));
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
  cout << "SimpleTest : start : scan nullptr"
    << " - " << k3.data()
    << " l_exclusive == false, "
    << "r_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, k.size(), false, k3.data(), k3.size(), false, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  cout << "SimpleTest : start : scan nullptr"
    << " - " << k6.data()
    << " l_exclusive == false, "
    << "r_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k6.data(), k6.size(), false, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  cout << "SimpleTest : start : scan nullptr"
    << " - " << k6.data()
    << " l_exclusive == false, "
    << "r_exclusive == true" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k6.data(), k6.size(), true, records));
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
    << k.data() << " - nullptr"
    << " l_exclusive == false, "
    << "r_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, nullptr, k3.size(), false, records));
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
    << "nullptr - nullptr"
    << " l_exclusive == false, "
    << "r_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, k.size(), false, nullptr, k3.size(), false, records));
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
    << "nullptr - " << k5.data()
    << " l_exclusive == false, "
    << "r_exclusive == false" << endl;
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k5.data(), k5.size(), false, records));
  cout << "SimpleTest : records.size() " << records.size() << endl;
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    std::string output((*itr)->key.get(), (*itr)->len_key);
    cout << "SimpleTest : records["
      << records.end() - itr
      << "] : "
      << output << endl;
  }
  ASSERT_EQ(Status::OK, commit(s));
  delete_record(s, st, nullptr, 0);
  delete_record(s, st, k.data(), k.size());
  delete_record(s, st, k2.data(), k2.size());
  delete_record(s, st, k3.data(), k3.size());
  delete_record(s, st, k6.data(), k6.size());
  commit(s);
  leave(s);
}

TEST_F(SimpleTest, scan_with_null_char) {
  std::string k("a\0a", 3);
  std::string k2("a\0a/", 4);
  std::string k3("a\0a/c", 5);
  std::string k4("a\0ab", 4);
  std::string k5("a\0a0", 4);
  ASSERT_EQ(3, k.size());
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, upsert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, upsert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, upsert(s, st, k4.data(), k4.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k2.data(), k2.size(), false, k5.data(), k5.size(), true, records));
  EXPECT_EQ(2, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  delete_record(s, st, k.data(), k.size());
  delete_record(s, st, k2.data(), k2.size());
  delete_record(s, st, k3.data(), k3.size());
  delete_record(s, st, k4.data(), k4.size());
  ASSERT_EQ(Status::OK, commit(s));
}

TEST_F(SimpleTest, scan_key_then_search_key) {
  std::string k("a");
  std::string k2("a/");
  std::string k3("a/c");
  std::string k4("b");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  std::vector<Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, nullptr, 0, false, records));
  ASSERT_EQ(Status::OK, commit(s));
  for (auto itr = records.begin(); itr != records.end(); ++itr)
    cout << std::string((*itr)->key.get(), (*itr)->len_key) << endl;
  records.clear();
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, upsert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, upsert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, upsert(s, st, k4.data(), k4.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(), k4.size(), true, records));
  EXPECT_EQ(2, records.size());
  
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, search_key(s, st, k2.data(), k2.size(), &tuple));
  EXPECT_NE(nullptr, tuple);
  delete_record(s, st, k2.data(), k2.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(), k4.size(), true, records));
  EXPECT_EQ(1, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  delete_record(s, st, k.data(), k.size());
  delete_record(s, st, k3.data(), k3.size());
  delete_record(s, st, k4.data(), k4.size());
  ASSERT_EQ(Status::OK, commit(s));
}

TEST_F(SimpleTest, insert_delete_with_16chars) { 
  std::string k("testing_a0123456");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(), k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto* t : records) {
    ASSERT_EQ(Status::OK, delete_record(s, st, t->key.get(), t->len_key));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, nullptr, 0, false, records));
  for (auto* t : records) {
    ASSERT_EQ(Status::OK, delete_record(s, st, t->key.get(), t->len_key));
    NNN;
  }
  ASSERT_EQ(Status::OK, commit(s));
}

}
