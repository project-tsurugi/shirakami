/**
 * @file simpleTest.cpp
 */

#include <array>
#include <bitset>
#include <future>

#include "gtest/gtest.h"
#include "kvs/interface.h"

// shirakami-impl interface library
#include "compiler.h"
#include "scheme_local.h"
#include "tuple_local.h"

using namespace kvs;

namespace shirakami::testing {

class SimpleTest : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { kvs::init(); }  // NOLINT

  void TearDown() override { kvs::fin(); }
};

TEST_F(SimpleTest, project_root) {  // NOLINT
  std::cout << MAC2STR(PROJECT_ROOT) << std::endl;
  std::string str(MAC2STR(PROJECT_ROOT));  // NOLINT
  str.append("/log/");
  std::cout << str << std::endl;
}

TEST_F(SimpleTest, tidword) {  // NOLINT
  // check the alignment of union
  TidWord tidword;
  tidword.set_epoch(1);
  tidword.set_lock(true);
  [[maybe_unused]] uint64_t res = tidword.get_obj();
  // std::cout << std::bitset<64>(res) << std::endl;
}

TEST_F(SimpleTest, enter) {  // NOLINT
  std::array<Token, 2> s{nullptr, nullptr};
  ASSERT_EQ(Status::OK, enter(s[0]));
  ASSERT_EQ(Status::OK, enter(s[1]));
  ASSERT_EQ(Status::OK, leave(s[0]));
  ASSERT_EQ(Status::OK, leave(s[1]));
}

TEST_F(SimpleTest, leave) {  // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
  ASSERT_EQ(Status::ERR_INVALID_ARGS, leave(nullptr));
}

TEST_F(SimpleTest, insert) {  // NOLINT
  std::string k("aaa");       // NOLINT
  std::string v("bbb");       // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, abort(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  {
    Tuple* tuple{};
    char k2 = 0;
    ASSERT_EQ(Status::OK, insert(s, st, &k2, 1, v.data(), v.size()));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, search_key(s, st, &k2, 1, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
    ASSERT_EQ(Status::OK, commit(s));
  }
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, insert(s, st, nullptr, 0, v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, nullptr, 0, &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), 3), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, update) {  // NOLINT
  std::string k("aaa");       // NOLINT
  std::string v("aaa");       // NOLINT
  std::string v2("bbb");      // NOLINT
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            update(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(
      memcmp(tuple->get_value().data(), v.data(), tuple->get_value().size()),
      0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            update(s, st, k.data(), k.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, search) {  // NOLINT
  std::string k("aaa");       // NOLINT
  std::string v("bbb");       // NOLINT
  Token s{};
  Storage st{};
  Tuple* tuple{};
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

TEST_F(SimpleTest, upsert) {  // NOLINT
  std::string k("aaa");       // NOLINT
  std::string v("aaa");       // NOLINT
  std::string v2("bbb");      // NOLINT
  Token s{};
  Storage st{};
  Tuple* tuple{};
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

TEST_F(SimpleTest, delete_) {  // NOLINT
  std::string k("aaa");        // NOLINT
  std::string v("aaa");        // NOLINT
  std::string v2("bbb");       // NOLINT
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

TEST_F(SimpleTest, scan) {  // NOLINT
  std::string k("aaa");     // NOLINT
  std::string k2("aab");    // NOLINT
  std::string k3("aac");    // NOLINT
  std::string k4("aad");    // NOLINT
  std::string k5("aadd");   // NOLINT
  std::string k6("aa");     // NOLINT
  std::string v("bbb");     // NOLINT
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
  uint64_t ctr(0);
  ASSERT_EQ(records.size(), 3);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 2) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(),
                                 k4.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 2);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k3.data(),
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 3);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 2) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k3.data(),
                                 k3.size(), true, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 2);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, k.size(), false, k3.data(),
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 5);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(itr->get_key().size(), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
    } else if (ctr == 2) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 3) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 4) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k6.data(), k6.size(),
                                 false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 2);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(itr->get_key().size(), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k6.data(), k6.size(),
                                 true, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 1);
  for ([[maybe_unused]] auto&& itr : records) {
    if (ctr == 0) {
      // ASSERT_EQ(itr->get_key().data(), nullptr);
      /*
       * key which is nullptr was inserted, but itr->get_key().data() refer
       * record, so not nullptr.
       */
      ++ctr;
    }
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, nullptr,
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 3);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 2) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, k.size(), false, nullptr,
                                 k3.size(), false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 5);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(itr->get_key().size(), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
    } else if (ctr == 2) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 3) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 4) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, nullptr, 0, false, k5.data(), k5.size(),
                                 false, records));
  ctr = 0;
  ASSERT_EQ(records.size(), 5);
  for (auto&& itr : records) {
    if (ctr == 0) {
      ASSERT_EQ(itr->get_key().size(), 0);
    } else if (ctr == 1) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
    } else if (ctr == 2) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
    } else if (ctr == 3) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
    } else if (ctr == 4) {
      ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
    }
    ++ctr;
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, scan_with_null_char) {  // NOLINT
  std::string k("a\0a", 3);                // NOLINT
  std::string k2("a\0a/", 4);              // NOLINT
  std::string k3("a\0a/c", 5);             // NOLINT
  std::string k4("a\0ab", 4);              // NOLINT
  std::string k5("a\0a0", 4);              // NOLINT
  ASSERT_EQ(3, k.size());
  std::string v("bbb");  // NOLINT
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

TEST_F(SimpleTest, scan_key_then_search_key) {  // NOLINT
  std::string k("a");                           // NOLINT
  std::string k2("a/");                         // NOLINT
  std::string k3("a/c");                        // NOLINT
  std::string k4("b");                          // NOLINT
  std::string v("bbb");                         // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK,
            scan_key(s, st, nullptr, 0, false, nullptr, 0, false, records));
  ASSERT_EQ(Status::OK, commit(s));
  for (auto&& itr : records) {
    std::cout << std::string(itr->get_key().data(),  // NOLINT
                             itr->get_key().size())
              << std::endl;
  }
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
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            search_key(s, st, k2.data(), k2.size(), &tuple));
  EXPECT_NE(nullptr, tuple);
  delete_record(s, st, k2.data(), k2.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), true, k4.data(),
                                 k4.size(), true, records));
  EXPECT_EQ(1, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, insert_delete_with_16chars) {  // NOLINT
  std::string k("testing_a0123456");              // NOLINT
  std::string v("bbb");                           // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto&& t : records) {
    ASSERT_EQ(Status::OK,
              delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, insert_delete_with_10chars) {  // NOLINT
  std::string k("testing_a0");                    // NOLINT
  std::string v("bbb");                           // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k.data(),
                                 k.size(), false, records));
  EXPECT_EQ(1, records.size());
  for (auto&& t : records) {
    ASSERT_EQ(Status::OK,
              delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, concurrent_updates) {  // NOLINT
  struct S {
    static void prepare() {
      std::string k0("a");  // NOLINT
      std::string k("aa");  // NOLINT
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Storage st{};
      ASSERT_EQ(Status::OK, insert(s, st, k0.data(), k0.size(),
                                   reinterpret_cast<char*>(&v),  // NOLINT
                                   sizeof(std::int64_t)));
      ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(),
                                   reinterpret_cast<char*>(&v),  // NOLINT
                                   sizeof(std::int64_t)));
      ASSERT_EQ(Status::OK, commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
    static void run(bool& rc) {
      std::string k("aa");  // NOLINT
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Storage st{};
      Tuple* t{};
      ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &t));
      ASSERT_NE(nullptr, t);
      v = *reinterpret_cast<std::int64_t*>(  // NOLINT
          const_cast<char*>(t->get_value().data()));
      v++;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));  // NOLINT
      ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(),
                                   reinterpret_cast<char*>(&v),  // NOLINT
                                   sizeof(std::int64_t)));
      rc = (Status::OK == commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
    static void verify() {
      std::string k("aa");  // NOLINT
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Storage st{};
      Tuple* tuple{};
      ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
      ASSERT_NE(nullptr, tuple);
      v = *reinterpret_cast<std::int64_t*>(  // NOLINT
          const_cast<char*>(tuple->get_value().data()));
      ASSERT_EQ(10, v);
      ASSERT_EQ(Status::OK, commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
  };

  S::prepare();
  auto r1 = std::async(std::launch::async, [&] {
    for (int i = 0; i < 5; ++i) {  // NOLINT
      bool rc = false;
      S::run(rc);
      if (!rc) {
        --i;
        continue;
      }
    }
  });
  for (int i = 0; i < 5; ++i) {  // NOLINT
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

TEST_F(SimpleTest, read_local_write) {  // NOLINT
  std::string k("aaa");                 // NOLINT
  std::string v("bbb");                 // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  Tuple* tuple{};
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, read_write_read) {  // NOLINT
  std::string k("aaa");                // NOLINT
  std::string v("bbb");                // NOLINT
  std::string v2("ccc");               // NOLINT
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
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, double_write) {  // NOLINT
  std::string k("aaa");             // NOLINT
  std::string v("bbb");             // NOLINT
  std::string v2("ccc");            // NOLINT
  std::string v3("ddd");            // NOLINT
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
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v3.data(), v3.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, double_read) {  // NOLINT
  std::string k("aaa");            // NOLINT
  std::string v("bbb");            // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            search_key(s, st, k.data(), k.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, all_deletes) {     // NOLINT
  std::string k("testing_a0123456");  // NOLINT
  std::string v("bbb");               // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK,
            scan_key(s, st, nullptr, 0, false, nullptr, 0, false, records));
  for (auto&& t : records) {
    ASSERT_EQ(Status::OK,
              delete_record(s, st, t->get_key().data(), t->get_key().size()));
  }
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, open_scan_test) {  // NOLINT
  std::string k1("a");                // NOLINT
  std::string v1("0");                // NOLINT
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ScanHandle handle{};
  ScanHandle handle2{};
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            insert(s, st, k1.data(), k1.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  ASSERT_EQ(0, handle);
  ASSERT_EQ(Status::OK,
            open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle2));
  ASSERT_EQ(1, handle2);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, read_from_scan) {  // NOLINT
  std::string k("aaa");               // NOLINT
  std::string k2("aab");              // NOLINT
  std::string k3("aac");              // NOLINT
  std::string k4("aad");              // NOLINT
  std::string k5("aae");              // NOLINT
  std::string k6("aa");               // NOLINT
  std::string v1("bbb");              // NOLINT
  std::string v2("bbb");              // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, nullptr, 0, v1.data(), v1.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k.data(), k.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k2.data(), k2.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k3.data(), k3.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k5.data(), k5.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k6.data(), k6.size(), v2.data(), v2.size()));
  ScanHandle handle{};
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(),
                                  k4.size(), false, handle));
  /**
   * test
   * if read_from_scan detects self write(update, insert), it read from owns.
   */
  ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
            read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, close_scan(s, st, handle));
  ASSERT_EQ(Status::OK, commit(s));

  /**
   * test
   * if it calls read_from_scan with invalid handle, it returns
   * Status::ERR_INVALID_HANDLE. if read_from_scan read all records in cache
   * taken at open_scan, it returns Status::WARN_SCAN_LIMIT.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(),
                                  k4.size(), false, handle));
  ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_from_scan(s, st, 3, &tuple));
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(),
                                  k4.size(), false, handle));
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
   * if read_from_scan detects the record deleted by myself, it function
   * returns Status::WARN_ALREADY_DELETE.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(),
                                  k4.size(), false, handle));
  ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
  EXPECT_EQ(Status::WARN_ALREADY_DELETE, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, abort(s));

  /**
   * test
   * if read_from_scan detects the record deleted by others between open_scan
   * and read_from_scan, it function returns Status::ERR_ILLEGAL_STATE which
   * means reading deleted record.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k.data(), k.size(), false, k4.data(),
                                  k4.size(), false, handle));
  Token s2{};
  ASSERT_EQ(Status::OK, enter(s2));
  ASSERT_EQ(Status::OK, delete_record(s2, st, k.data(), k.size()));
  ASSERT_EQ(Status::OK, commit(s2));
  EXPECT_EQ(Status::WARN_CONCURRENT_DELETE,
            read_from_scan(s, st, handle, &tuple));

  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(SimpleTest, close_scan) {  // NOLINT
  std::string k1("a");            // NOLINT
  std::string v1("0");            // NOLINT
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ScanHandle handle{};
  ASSERT_EQ(Status::OK,
            insert(s, st, k1.data(), k1.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK,
            open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  ASSERT_EQ(Status::OK, close_scan(s, st, handle));
  ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, st, handle));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, mixing_scan_and_search) {  // NOLINT
  std::string k1("aaa");                      // NOLINT
  std::string k2("aab");                      // NOLINT
  std::string k3("xxx");                      // NOLINT
  std::string k4("zzz");                      // NOLINT
  std::string v1("bbb");                      // NOLINT
  std::string v2("bbb");                      // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK,
            insert(s, st, k1.data(), k1.size(), v1.data(), v1.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k2.data(), k2.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK,
            insert(s, st, k4.data(), k4.size(), v2.data(), v2.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ScanHandle handle{};
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, open_scan(s, st, k1.data(), k1.size(), false, k2.data(),
                                  k2.size(), false, handle));
  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(memcmp(tuple->get_key().data(), k1.data(), k1.size()), 0);
  ASSERT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);

  // record exists
  ASSERT_EQ(Status::OK, search_key(s, st, k4.data(), k4.size(), &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);

  // record not exist
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            search_key(s, st, k3.data(), k3.size(), &tuple));

  ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(memcmp(tuple->get_key().data(), k2.data(), k2.size()), 0);
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, commit(s));

  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(SimpleTest, long_insert) {  // NOLINT
  std::string k("CUSTOMER");       // NOLINT
  std::string v(                   // NOLINT
      "b234567890123456789012345678901234567890123456789012345678901234567890"
      "12"
      "3456789012345678901234567890123456789012345678901234567890123456789012"
      "34"
      "5678901234567890123456789012345678901234567890123456789012345678901234"
      "56"
      "7890123456789012345678901234567890123456789012345678901234567890123456"
      "78"
      "9012345678901234567890123456789012345678901234567890123456789012345678"
      "90"
      "1234567890123456789012345678901234567890123456789012345678901234567890"
      "12"
      "3456789012345678901234567890123456789012345678901234567890123456789012"
      "34"
      "5678901234567890123456789012345678901234567890123456789012345678901234"
      "56"
      "7890123456789012345678901234567890123456789012345678901234567890123456"
      "78"
      "9012345678901234567890123456789012345678901234567890123456789012345678"
      "90"
      "1234567890123456789012345678901234567890123456789012345678901234567890"
      "12"
      "3456789012345678901234567890123456789012345678901234567890123456789012"
      "34"
      "5678901234567890123456789012345678901234567890");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing
