#include <xmmintrin.h>

#include <array>
#include <bitset>

// apt
#include <glog/logging.h>

#include "compiler.h"
#include "concurrency_control/include/epoch.h"
#include "tuple_local.h"

#include "shirakami/interface.h"

// third party
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};

class upsert_after_delete : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/upsert_after_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(upsert_after_delete, txs) { // NOLINT
    google::InitGoogleLogging("shirakami-test-concurrency_control-upsert-upsert_after_insert_test");
    register_storage(st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s1, st, k, v));
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT, upsert(s2, st, k, v));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    Tuple *tuple{};
    ASSERT_EQ(Status::OK, search_key(s1, st, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
