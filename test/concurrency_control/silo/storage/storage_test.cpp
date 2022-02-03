
#include "gtest/gtest.h"

// shirakami-impl interface library
#include "concurrency_control/silo/include/tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class storage : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/storage_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(storage, multiple_storages) { // NOLINT
    Storage storage0{};
    Storage storage1{};
    ASSERT_EQ(Status::OK, register_storage(storage0));
    ASSERT_EQ(Status::OK, register_storage(storage1));
    std::string k("k");   // NOLINT
    std::string v0("v0"); // NOLINT
    std::string v1("v1"); // NOLINT
    Token token{};
    ASSERT_EQ(Status::OK, enter(token));
    ASSERT_EQ(Status::OK, upsert(token, storage0, k, v0));
    ASSERT_EQ(Status::OK, upsert(token, storage1, k, v0));
    ASSERT_EQ(Status::OK, commit(token)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, upsert(token, storage0, k, v1));
    ASSERT_EQ(Status::OK, search_key(token, storage1, k, tuple));
    std::string val{};
    tuple->get_value(val);
    ASSERT_EQ(memcmp(val.data(), v0.data(), v0.size()), 0);
    ASSERT_EQ(Status::OK, commit(token)); // NOLINT
}

} // namespace shirakami::testing
