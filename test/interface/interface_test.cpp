#include "kvs/interface.h"

#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "cc/silo_variant/include/scheme.h"
#include "compiler.h"
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class interface : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override {
    fin();
  }
};

TEST_F(interface, project_root) {  // NOLINT
  std::cout << MAC2STR(PROJECT_ROOT) << std::endl;
  std::string str(MAC2STR(PROJECT_ROOT));  // NOLINT
  str.append("/log/");
  std::cout << str << std::endl;
}

TEST_F(interface, tidword) {  // NOLINT
  // check the alignment of union
  tid_word tidword;
  tidword.set_epoch(1);
  tidword.set_lock(true);
  [[maybe_unused]] uint64_t res = tidword.get_obj();
  // std::cout << std::bitset<64>(res) << std::endl;
}

TEST_F(interface, enter) {  // NOLINT
  std::array<Token, 2> s{nullptr, nullptr};
  ASSERT_EQ(Status::OK, enter(s[0]));
  ASSERT_EQ(Status::OK, enter(s[1]));
  ASSERT_EQ(Status::OK, leave(s[0]));
  ASSERT_EQ(Status::OK, leave(s[1]));
}

TEST_F(interface, leave) {  // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
  ASSERT_EQ(Status::ERR_INVALID_ARGS, leave(nullptr));
}

TEST_F(interface, search) {  // NOLINT
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

TEST_F(interface, upsert) {  // NOLINT
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

TEST_F(interface, delete_) {  // NOLINT
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

TEST_F(interface, scan_key_then_search_key) {  // NOLINT
  std::string k("a");                           // NOLINT
  std::string k2("aa");                         // NOLINT
  std::string k3("aac");                        // NOLINT
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

TEST_F(interface, insert_delete_with_16chars) {  // NOLINT
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

TEST_F(interface, insert_delete_with_10chars) {  // NOLINT
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

TEST_F(interface, read_local_write) {  // NOLINT
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

TEST_F(interface, read_write_read) {  // NOLINT
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

TEST_F(interface, double_write) {  // NOLINT
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

TEST_F(interface, double_read) {  // NOLINT
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

TEST_F(interface, all_deletes) {     // NOLINT
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

TEST_F(interface, mixing_scan_and_search) {  // NOLINT
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

}  // namespace shirakami::testing
