#include <bitset>

#include "gtest/gtest.h"
#include "kvs/interface.h"
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class simple_scan : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override { fin(); }
};

TEST_F(simple_scan, scan) {  // NOLINT
  std::string k("aaa");      // NOLINT
  std::string k2("aab");     // NOLINT
  std::string k3("aac");     // NOLINT
  std::string k4("aad");     // NOLINT
  std::string k5("aadd");    // NOLINT
  std::string k6("aa");      // NOLINT
  std::string v("bbb");      // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, "", v));
  ASSERT_EQ(Status::OK, insert(s, st, k, v));
  ASSERT_EQ(Status::OK, insert(s, st, k2, v));
  ASSERT_EQ(Status::OK, insert(s, st, k3, v));
  ASSERT_EQ(Status::OK, insert(s, st, k6, v));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k, false, k4, false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, k, true, k4, false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, k, false, k3, false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, k, false, k3, true, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, "", false, k3, false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, "", false, k6, false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, "", false, k6, true, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, k, false, "", false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, "", false, "", false, records));
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
  ASSERT_EQ(Status::OK, scan_key(s, st, "", false, k5, false, records));
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

TEST_F(simple_scan, scan_with_null_char) {  // NOLINT
  std::string k("a\0a", 3);                 // NOLINT
  std::string k2("a\0aa", 4);               // NOLINT
  std::string k3("a\0aac", 5);              // NOLINT
  std::string k4("a\0ab", 4);               // NOLINT
  std::string k5("a\0ac", 4);               // NOLINT
  ASSERT_EQ(3, k.size());
  std::string v("bbb");  // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, upsert(s, st, k, v));
  ASSERT_EQ(Status::OK, upsert(s, st, k2, v));
  ASSERT_EQ(Status::OK, upsert(s, st, k3, v));
  ASSERT_EQ(Status::OK, upsert(s, st, k4, v));
  ASSERT_EQ(Status::OK, commit(s));
  std::vector<const Tuple*> records{};
  ASSERT_EQ(Status::OK, scan_key(s, st, k2, false, k5, true, records));
  EXPECT_EQ(3, records.size());
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, open_scan_test) {  // NOLINT
  std::string k1("a");                 // NOLINT
  std::string v1("0");                 // NOLINT
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ScanHandle handle{};
  ScanHandle handle2{};
  ASSERT_EQ(Status::WARN_NOT_FOUND,
            open_scan(s, st, "", false, "", false, handle));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, st, k1, v1));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, open_scan(s, st, "", false, "", false, handle));
  ASSERT_EQ(0, handle);
  ASSERT_EQ(Status::OK, open_scan(s, st, "", false, "", false, handle2));
  ASSERT_EQ(1, handle2);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, read_from_scan) {  // NOLINT
  std::string k("aaa");                // NOLINT
  std::string k2("aab");               // NOLINT
  std::string k3("aac");               // NOLINT
  std::string k4("aad");               // NOLINT
  std::string k5("aae");               // NOLINT
  std::string k6("aa");                // NOLINT
  std::string v1("bbb");               // NOLINT
  std::string v2("bbb");               // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  ASSERT_EQ(Status::OK, insert(s, st, "", v1));
  ASSERT_EQ(Status::OK, insert(s, st, k, v1));
  ASSERT_EQ(Status::OK, insert(s, st, k2, v2));
  ASSERT_EQ(Status::OK, insert(s, st, k3, v1));
  ASSERT_EQ(Status::OK, insert(s, st, k5, v1));
  ASSERT_EQ(Status::OK, insert(s, st, k6, v2));
  ScanHandle handle{};
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, open_scan(s, st, k, false, k4, false, handle));
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
  ASSERT_EQ(Status::OK, open_scan(s, st, k, false, k4, false, handle));
  ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_from_scan(s, st, 3, &tuple));
  ASSERT_EQ(Status::OK, open_scan(s, st, k, false, k4, false, handle));
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
  ASSERT_EQ(Status::OK, open_scan(s, st, k, false, k4, false, handle));
  ASSERT_EQ(Status::OK, delete_record(s, st, k));
  EXPECT_EQ(Status::WARN_ALREADY_DELETE, read_from_scan(s, st, handle, &tuple));
  ASSERT_EQ(Status::OK, abort(s));

  /**
   * test
   * if read_from_scan detects the record deleted by others between open_scan
   * and read_from_scan, it function returns Status::ERR_ILLEGAL_STATE which
   * means reading deleted record.
   */
  ASSERT_EQ(Status::OK, open_scan(s, st, k, false, k4, false, handle));
  Token s2{};
  ASSERT_EQ(Status::OK, enter(s2));
  ASSERT_EQ(Status::OK, delete_record(s2, st, k));
  ASSERT_EQ(Status::OK, commit(s2));
  EXPECT_EQ(Status::WARN_CONCURRENT_DELETE,
            read_from_scan(s, st, handle, &tuple));

  ASSERT_EQ(Status::OK, leave(s));
  ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(simple_scan, close_scan) {  // NOLINT
  std::string k1("a");             // NOLINT
  std::string v1("0");             // NOLINT
  Token s{};
  Storage st{};
  ASSERT_EQ(Status::OK, enter(s));
  ScanHandle handle{};
  ASSERT_EQ(Status::OK, insert(s, st, k1, v1));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, open_scan(s, st, "", false, "", false, handle));
  ASSERT_EQ(Status::OK, close_scan(s, st, handle));
  ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, st, handle));
  ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing