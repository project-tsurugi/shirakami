#include <bitset>

#include "gtest/gtest.h"
#include "kvs/interface.h"
#include "tuple_local.h"

#if defined(RECOVERY)

#include "boost/filesystem.hpp"

#endif

using namespace shirakami;

namespace shirakami::testing {

class scan_insert_test : public ::testing::Test {  // NOLINT
public:
    void SetUp() override {
#if defined(RECOVERY)
        std::string path{MAC2STR(PROJECT_ROOT)}; // NOLINT
        path += "/log/checkpoint";
        if (boost::filesystem::exists(path)) {
            boost::filesystem::remove(path);
        }
#endif
        init();  // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(scan_insert_test, insert_record_after_select_doesnt_find) {  // NOLINT
  std::string k0("k0");                       // NOLINT
  std::string k1("k1");                       // NOLINT
  std::string v("v");                              // NOLINT
  Token s0{};
  ASSERT_EQ(Status::OK, enter(s0));
  ASSERT_EQ(Status::OK, insert(s0, k0, v));

  Token s1{};
  ASSERT_EQ(Status::OK, enter(s1));
  Tuple* t{};
  ScanHandle handle;
  ASSERT_EQ(Status::OK, open_scan(
      s1,
      k0, scan_endpoint::INCLUSIVE,
      k1, scan_endpoint::INCLUSIVE,
      handle)
  );
  ASSERT_EQ(Status::WARN_CONCURRENT_INSERT, read_from_scan(
      s1,
      handle,
      &t)
  );
  ASSERT_FALSE(t); // NOLINT
  ASSERT_EQ(Status::OK, close_scan(s1, handle));
  ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
  ASSERT_EQ(Status::OK, leave(s1));
  ASSERT_EQ(Status::OK, insert(s0, k1, v));
  ASSERT_EQ(Status::OK, commit(s0)); // NOLINT
  ASSERT_EQ(Status::OK, leave(s0));
}

}  // namespace shirakami::testing
