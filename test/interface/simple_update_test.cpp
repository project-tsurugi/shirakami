#include <bitset>
#include <future>

#include "gtest/gtest.h"
#include "kvs/interface.h"

// shirakami-impl simple_update library
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class simple_update : public ::testing::Test {  // NOLINT
public:
  void SetUp() override { init(); }  // NOLINT

  void TearDown() override { fin(); }
};

TEST_F(simple_update, update) {  // NOLINT
  std::string k("aaa");          // NOLINT
  std::string v("aaa");          // NOLINT
  std::string v2("bbb");         // NOLINT
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, k, v));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, insert(s, k, v));
  ASSERT_EQ(Status::OK, commit(s));
  Tuple* tuple{};
  ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
  ASSERT_EQ(
      memcmp(tuple->get_value().data(), v.data(), tuple->get_value().size()),
      0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, update(s, k, v2));
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
  ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
  ASSERT_EQ(Status::OK, commit(s));
  ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_update, concurrent_updates) {  // NOLINT
  struct S {
    static void prepare() {
      std::string k0("a");  // NOLINT
      std::string k("aa");  // NOLINT
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      ASSERT_EQ(Status::OK, insert(s, k0,
                                   {reinterpret_cast<char*>(&v),  // NOLINT
                                    sizeof(std::int64_t)}));
      ASSERT_EQ(Status::OK, insert(s, k,
                                   {reinterpret_cast<char*>(&v),  // NOLINT
                                    sizeof(std::int64_t)}));
      ASSERT_EQ(Status::OK, commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
    static void run(bool& rc) {
      std::string k("aa");  // NOLINT
      std::int64_t v{0};
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Tuple* t{};
      ASSERT_EQ(Status::OK, search_key(s, k, &t));
      ASSERT_NE(nullptr, t);
      v = *reinterpret_cast<std::int64_t*>(  // NOLINT
          const_cast<char*>(t->get_value().data()));
      v++;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));  // NOLINT
      ASSERT_EQ(Status::OK, upsert(s, k,
                                   {reinterpret_cast<char*>(&v),  // NOLINT
                                    sizeof(std::int64_t)}));
      rc = (Status::OK == commit(s));
      ASSERT_EQ(Status::OK, leave(s));
    }
    static void verify() {
      std::string k("aa");  // NOLINT
      Token s{};
      ASSERT_EQ(Status::OK, enter(s));
      Tuple* tuple{};
      ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
      ASSERT_NE(nullptr, tuple);
      std::int64_t v{*reinterpret_cast<std::int64_t*>(  // NOLINT
          const_cast<char*>(tuple->get_value().data()))};
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

}  // namespace shirakami::testing
