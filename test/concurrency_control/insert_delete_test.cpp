#include <bitset>

#include "tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

class insert_delete : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/insert_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

Storage storage;

TEST_F(insert_delete, insert_delete_with_16chars) { // NOLINT
    ASSERT_EQ(register_storage(storage), Status::OK);
    std::string k("testing_a0123456"); // NOLINT
    std::string v("bbb");              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> tuples{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, tuples));
    EXPECT_EQ(1, tuples.size());
    for (auto&& t : tuples) {
        ASSERT_EQ(Status::OK, delete_record(s, storage, t->get_key()));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, insert_delete_with_10chars) { // NOLINT
    ASSERT_EQ(register_storage(storage), Status::OK);
    std::string k("testing_a0"); // NOLINT
    std::string v("bbb");        // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, records));
    EXPECT_EQ(1, records.size());
    for (auto&& t : records) {
        ASSERT_EQ(Status::OK, delete_record(s, storage, t->get_key()));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, delete_insert) { // NOLINT
    register_storage(storage);
    std::string k("testing"); // NOLINT
    std::string v("bbb");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* t{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, delete_insert_on_scan) { // NOLINT
    register_storage(storage);
    std::string k("testing");  // NOLINT
    std::string k2("new_key"); // NOLINT
    std::string v("bbb");      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* t{};
    ScanHandle handle;
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k2, &t));
    ASSERT_TRUE(t); // NOLINT
    Status ret{search_key(s, storage, k, &t)};
    ASSERT_TRUE(ret == Status::WARN_NOT_FOUND || ret == Status::WARN_CONCURRENT_DELETE); // NOLINT
    /**
     * Status::WARN_CONCURRENT_DELETE : Detected records that were deleted but remained in the index so that CPR threads
     * could be found, or read only snapshot transactions could be found.
     */
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete, delete_upsert) { // NOLINT
    register_storage(storage);
    std::string k("testing"); // NOLINT
    std::string v("bbb");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* t{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &t));
    ASSERT_TRUE(t); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
