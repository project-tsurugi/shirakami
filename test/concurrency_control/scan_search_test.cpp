/**
 * @file scan_search_test.cpp
 */

#include <bitset>

#include "gtest/gtest.h"

#include "tuple_local.h"

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

#include "boost/filesystem.hpp"

#if defined(RECOVERY)

#include "fault_tolerance/include/cpr.h"

#endif

#include "shirakami/interface.h"

using namespace shirakami;

namespace shirakami::testing {

class scan_search : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/scan_search_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override {
#if defined(RECOVERY)
        shirakami::cpr::wait_next_checkpoint();
#endif
        fin();
    }
};


TEST_F(scan_search, scan_key_search_key) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("a");    // NOLINT
    std::string k2("aa");  // NOLINT
    std::string k3("aac"); // NOLINT
    std::string k4("b");   // NOLINT
    std::string v("bbb");  // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::vector<const Tuple*> records{};
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::EXCLUSIVE, records));
#endif
    EXPECT_EQ(0, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, k4, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::EXCLUSIVE, records));
#endif
    EXPECT_EQ(2, records.size());

    Tuple* tuple{};
    while (Status::OK != search_key(s, storage, k2, &tuple)) {
        ;
    }
    EXPECT_NE(nullptr, tuple);
    delete_record(s, storage, k2);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    /**
     * cpr checkpoint thread may delete concurrently by upper 2 line's delete_record function.
     */
    cpr::wait_next_checkpoint();
#endif
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
    EXPECT_EQ(1, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k3));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k4));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(scan_search, mixing_scan_and_search) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k1("aaa"); // NOLINT
    std::string k2("aab"); // NOLINT
    std::string k3("xxx"); // NOLINT
    std::string k4("zzz"); // NOLINT
    std::string v1("bbb"); // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v2));
    ASSERT_EQ(Status::OK, insert(s, storage, k4, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle handle{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::INCLUSIVE, handle));
#ifdef CPR
    while (Status::OK != read_from_scan(s, handle, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_key().data(), k1.data(), k1.size()), 0);
    ASSERT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);

// record exists
#ifdef CPR
    while (Status::OK != search_key(s, storage, k4, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k4, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);

    // record not exist
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k3, &tuple));

#ifdef CPR
    while (Status::OK != read_from_scan(s, handle, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
    ASSERT_EQ(memcmp(tuple->get_key().data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k2));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k4));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
