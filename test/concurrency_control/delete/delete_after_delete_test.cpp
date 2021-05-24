#include "shirakami/interface.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class delete_after_delete : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/delete_after_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(delete_after_delete, delete_after_delete) { // NOLINT
    register_storage(storage);
    std::string k1("k");  // NOLINT
    std::string v1("v1"); // NOLINT
    Token s{};
    for (std::size_t i = 0; i < 100; ++i) { // NOLINT
        /**
         * Test the interaction between snapshot manager and cpr manager by inserting enter / leave frequently.
         */
        ASSERT_EQ(Status::OK, enter(s));
        while (Status::WARN_NOT_FOUND == upsert(s, storage, k1, v1)) {
            ;
            /**
             * The last delete_record didn't unhook the record because of the snapshot manager or cpr manager, 
             * so upsert is an update instruction because the record exists, and WARN_NOT_FOUND because it was a deleted record.
             */
            commit(s);
            /**
             * If you don't commit, the epoch and manager may not progress, so you may end up in an infinite loop here.
             */
        }
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK, leave(s));
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::OK, delete_record(s, storage, k1));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK, leave(s));
        ASSERT_EQ(Status::OK, enter(s));
        ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, storage, k1));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK, leave(s));
    }
}

} // namespace shirakami::testing
