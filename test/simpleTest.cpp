
#include "gtest/gtest.h"

#include "kvs/interface.h"

// kvs_charkey-impl interface library
#include "compiler.hh"
#include "debug.hh"
#include "scheme.hh"
#include "xact.hh"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class SimpleTest : public ::testing::Test {
 protected:
  SimpleTest() { kvs::init(); }
  ~SimpleTest() { 
    kvs::fin(); 
    kvs::delete_all_records();
    kvs::delete_all_garbage_records();
    //kvs::MTDB.destroy();
  }
};

TEST_F(SimpleTest, project_root) {
  cout << MAC2STR(PROJECT_ROOT) << endl;
  std::string str(MAC2STR(PROJECT_ROOT));
  str.append("/log/");
  cout << str << endl;
}

TEST_F(SimpleTest, tidword) {
  // check the alignment of union
  TidWord tidword;
  tidword.set_epoch(1);
  tidword.set_lock(true);
  uint64_t res = tidword.get_obj();
  //cout << std::bitset<64>(res) << endl;
}

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
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  {
    Tuple* tuple;
    char k2 = 0;
    ASSERT_EQ(Status::OK, insert(s, st, &k2, 1, v.data(), v.size()));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, search_key(s, st, &k2, 1, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
    ASSERT_EQ(Status::OK, commit(s));
  }
  Tuple* tuple;
  ASSERT_EQ(Status::OK, insert(s, st, nullptr, 0, v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, nullptr, 0, &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
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
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            update(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple;
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), tuple->get_value().size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            update(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, search) {
  std::string k("aaa");
  std::string v("bbb");
  Token s{};
  Storage st{};
  Tuple* tuple;
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, upsert) {
  std::string k("aaa");
  std::string v("aaa");
  std::string v2("bbb");
  Token s{};
  Storage st{};
  Tuple* tuple;
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::WARN_ALREADY_EXISTS,
            insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
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
  ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, st, k.data(), k.size()));
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
  ASSERT_EQ(Status::OK, insert(s, st, nullptr, 0, v.data(), v.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k6.data(), k6.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k4.data(),
                                 k4.size(), false, records));
  ThreadInfo* ti = static_cast<ThreadInfo*>(s);
  ti->display_read_set();
  uint64_t ctr(0);
  ASSERT_EQ(records.size(), 3);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0) {
      std::string_view key_view = (*itr)->get_key();
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    }
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 2)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(),
                                 k4.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 2);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k3.data(),
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 3);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 2)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k3.data(),
                                 k3.size(), true, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 2);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, k.size(), false, k3.data(),
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 5);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0);
      //ASSERT_EQ(memcmp((*itr)->get_key().data(), nullptr, 0), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k6.data(), k6.size()), 0);
    else if (ctr == 2)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    else if (ctr == 3)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 4)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k6.data(), k6.size(),
                                 false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 2);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0);
      //ASSERT_EQ(memcmp((*itr)->get_key().data(), nullptr, 0), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k6.data(), k6.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k6.data(), k6.size(),
                                 true, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 1);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0) //ASSERT_EQ(memcmp((*itr)->get_key().data(), nullptr, 0), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, nullptr,
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 3);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 2)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, k.size(), false, nullptr,
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 5);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0);
      //ASSERT_EQ(memcmp((*itr)->get_key().data(), nullptr, 0), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k6.data(), k6.size()), 0);
    else if (ctr == 2)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    else if (ctr == 3)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 4)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k5.data(), k5.size(),
                                 false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 5);
  for (auto itr = records.begin(); itr != records.end(); ++itr) {
    if (ctr == 0);
      //ASSERT_EQ(memcmp((*itr)->get_key().data(), nullptr, 0), 0);
    else if (ctr == 1)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k6.data(), k6.size()), 0);
    else if (ctr == 2)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k.data(), k.size()), 0);
    else if (ctr == 3)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k2.data(), k2.size()), 0);
    else if (ctr == 4)
      ASSERT_EQ(memcmp((*itr)->get_key().data(), k3.data(), k3.size()), 0);
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
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
  ASSERT_EQ(Status::OK,
            upsert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k4.data(), k4.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k2.data(), k2.size(), false, k5.data(),
                                 k5.size(), true, records));
  EXPECT_EQ(2, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
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
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK,
            scan_key(s, st, nullptr, 0, false, nullptr, 0, false, records));
  ASSERT_EQ(Status::OK, commit(s));
  for (auto itr = records.begin(); itr != records.end(); ++itr)
    cout << std::string((*itr)->get_key().data(), (*itr)->get_key().size()) << endl;
  records.clear();
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k2.data(), k2.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k3.data(), k3.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK,
            upsert(s, st, k4.data(), k4.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(),
                                 k4.size(), true, records));
  EXPECT_EQ(2, records.size());

  Tuple* tuple{};
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k2.data(), k2.size(), &tuple));
  EXPECT_NE(nullptr, tuple);
  delete_record(s, st, k2.data(), k2.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(),
                                 k4.size(), true, records));
  EXPECT_EQ(1, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, insert_delete_with_16chars) {
  std::string k("testing_a0123456");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto* t : records) {
    ASSERT_EQ(Status::OK, delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, insert_delete_with_10chars) {
  std::string k("testing_a0");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto* t : records) {
    ASSERT_EQ(Status::OK, delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, concurrent_updates) {
  struct S {
    static void prepare() {
      std::string k0("a");
      std::string k("aa");
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Storage st{};
      ASSERT_EQ(Status::OK,
                insert(s, st, k0.data(), k0.size(), reinterpret_cast<char*>(&v),
                       sizeof(std::int64_t)));
      ASSERT_EQ(Status::OK,
                insert(s, st, k.data(), k.size(), reinterpret_cast<char*>(&v),
                       sizeof(std::int64_t)));
      ASSERT_EQ(Status::OK, commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
    static void run(bool& rc) {
      std::string k("aa");
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Storage st{};
      Tuple* t{};
      ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &t));
      ASSERT_NE(nullptr, t);
      v = *reinterpret_cast<std::int64_t*>(const_cast<char*>(t->get_value().data()));
      v++;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      ASSERT_EQ(Status::OK,
                upsert(s, st, k.data(), k.size(), reinterpret_cast<char*>(&v),
                       sizeof(std::int64_t)));
      rc = (Status::OK == commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
    static void verify() {
      std::string k("aa");
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Storage st{};
      Tuple* t{};
      ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &t));
      ASSERT_NE(nullptr, t);
      v = *reinterpret_cast<std::int64_t*>(const_cast<char*>(t->get_value().data()));
      EXPECT_EQ(10, v);
      ASSERT_EQ(Status::OK, commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
  };

  S::prepare();
  auto r1 = std::async(std::launch::async, [&] {
    for (std::size_t i = 0U; i < 5U; ++i) {
      bool rc = false;
      S::run(rc);
      if (!rc) {
        --i;
        continue;
      }
    }
  });
  for (std::size_t i = 0U; i < 5U; ++i) {
    bool rc = false;
    S::run(rc);
    if (!rc) {
      --i;
      continue;
    }
  }
  r1.wait();
  S::verify();
}

TEST_F(SimpleTest, read_local_write) {
  std::string k("aaa");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  Tuple* tuple{};
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, read_write_read) {
  std::string k("aaa");
  std::string v("bbb");
  std::string v2("ccc");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::OK,
            upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, double_write) {
  std::string k("aaa");
  std::string v("bbb");
  std::string v2("ccc");
  std::string v3("ddd");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple{};
  ASSERT_EQ(Status::OK,
            upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE,
            upsert(s, st, k.data(), k.size(), v3.data(), v3.size()));
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v3.data(), v3.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, double_read) {
  std::string k("aaa");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, all_deletes) {
  std::string k("testing_a0123456");
  std::string v("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK,
            scan_key(s, st, nullptr, 0, false, nullptr, 0, false, records));
  for (auto* t : records) {
    ASSERT_EQ(Status::OK, delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  forced_gc_all_records();
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, open_scan_test) {
  std::string k1("a");
  std::string v1("0");
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ScanHandle handle{}, handle2{};
  ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k1.data(), k1.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  ASSERT_EQ(0, handle);
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle2));
  ASSERT_EQ(1, handle2);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, read_from_scan) {
  std::string k("aaa");
  std::string k2("aab");
  std::string k3("aac");
  std::string k4("aad");
  std::string k5("aae");
  std::string k6("aa");
  std::string v1("bbb");
  std::string v2("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, nullptr, 0, v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k2.data(), k2.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k3.data(), k3.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k5.data(), k5.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k6.data(), k6.size(), v2.data(), v2.size()));
  ScanHandle handle{};
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(), k4.size(), false, handle));
  /**
   * test
   * if read_from_scan detects self write(update, insert), it read from owns.
   */
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, close_scan(s, st, handle));
  ASSERT_EQ(Status::OK, commit(s));

  /**
   * test
   * if it calls read_from_scan with invalid handle, it returns Status::ERR_INVALID_HANDLE.
   * if read_from_scan read all records in cache taken at open_scan, it returns Status::WARN_SCAN_LIMIT.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(), k4.size(), false, handle));
  ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_from_scan(s, st, 3, &tuple));
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(), k4.size(), false, handle));
  EXPECT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  EXPECT_EQ(memcmp(tuple->get_key().data(), k.data(), k.size()), 0);
  EXPECT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);
  EXPECT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  EXPECT_EQ(memcmp(tuple->get_key().data(), k2.data(), k2.size()), 0);
  EXPECT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  EXPECT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  EXPECT_EQ(memcmp(tuple->get_key().data(), k3.data(), k3.size()), 0);
  EXPECT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);
  EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, commit(s));

  /**
   * test
   * if read_from_scan detects the record deleted by myself, it function returns Status::WARN_ALREADY_DELETE.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(), k4.size(), false, handle));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  EXPECT_EQ(Status::WARN_ALREADY_DELETE, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, abort(s));

  /**
   * test
   * if read_from_scan detects the record deleted by others between open_scan and read_from_scan,
   * it function returns Status::ERR_ILLEGAL_STATE which means reading deleted record.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(), k4.size(), false, handle));
  Token s2{};
  ASSERT_EQ(Status::OK, enter(s2));
  ASSERT_EQ(Status::OK, delete_record(s2, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s2));
  EXPECT_EQ(Status::WARN_CONCURRENT_DELETE, read_from_scan(s, st, handle, &tuple));

  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(SimpleTest, close_scan) {
  std::string k1("a");
  std::string v1("0");
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ScanHandle handle{};
  ASSERT_EQ(Status::OK, insert(s, st, k1.data(), k1.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  ASSERT_EQ(Status::OK, close_scan(s, st, handle));
  ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, st, handle));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, mixing_scan_and_search) {
  std::string k1("aaa");
  std::string k2("aab");
  std::string k3("xxx");
  std::string k4("zzz");
  std::string v1("bbb");
  std::string v2("bbb");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k1.data(), k1.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k2.data(), k2.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, insert(s, st, k4.data(), k4.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ScanHandle handle{};
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, open_scan(s, st, k1.data(), k1.size(), false, k2.data(), k2.size(), false, handle));
  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(memcmp(tuple->get_key().data(), k1.data(), k1.size()), 0);
  ASSERT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);

  // record exists
  ASSERT_EQ(Status::OK, search_key(s, st, k4.data(), k4.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);

  // record not exist
  ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, k3.data(), k3.size(), &tuple));

  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(memcmp(tuple->get_key().data(), k2.data(), k2.size()), 0);
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, commit(s));

  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, long_insert) {
  std::string k("CUSTOMER");
  std::string v("b234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace kvs_charkey::testing
