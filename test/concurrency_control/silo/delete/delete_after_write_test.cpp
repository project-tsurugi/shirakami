#include <bitset>

#include "tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class delete_after_write : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/delete_after_write_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(delete_after_write, delete_after_insert) { // NOLINT
    register_storage(storage);
    std::string k1("k"); // NOLINT
    std::string v1("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT, delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(delete_after_write, delete_after_upsert) { // NOLINT
    register_storage(storage);
    std::string k1("k"); // NOLINT
    std::string v1("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k1, v1));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT, delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(delete_after_write, delete_after_update) { // NOLINT
    register_storage(storage);
    std::string k1("k");  // NOLINT
    std::string v1("v1"); // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, update(s, storage, k1, v2));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_OPERATION, delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, commit(s));
    Tuple* tuple{};
#ifdef CPR
    while (Status::WARN_NOT_FOUND != search_key(s, storage, k1, &tuple)) {
        ;
    }
#else
    while (Status::WARN_NOT_FOUND != search_key(s, storage, k1, &tuple)) {
        ;
    }
#endif
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
