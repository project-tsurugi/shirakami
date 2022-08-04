#include <xmmintrin.h>

#include <array>
#include <bitset>
#include <mutex>

// apt
#include <glog/logging.h>

#include "compiler.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"


// third party
#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};

class upsert_after_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-upsert_after_insert_test");
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(upsert_after_delete, txs) { // NOLINT
    create_storage("", st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s1, st, k, v));
    ASSERT_EQ(Status::OK, upsert(s2, st, k, v));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s1, st, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
