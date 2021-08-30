#include <bitset>

#include "concurrency_control/silo/include/tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/simple_scan_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, scan_with_prefixed_end) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("T6\000\200\000\000\n\200\000\000\001", 11); // NOLINT
    std::string end("T6\001", 3);                              // NOLINT
    std::string v("bbb");                                      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, "", scan_endpoint::INF, end, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, end, scan_endpoint::EXCLUSIVE, records));
#endif
    EXPECT_EQ(1, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_range_endpoint1) { // NOLINT
    Storage storage{};
    register_storage(storage);
    // simulating 1st case in umikongo OperatorTest scan_pushdown_range
    std::string r1("T200\x00\x80\x00\x00\xc7\x80\x00\x01\x91\x80\x00\x01\x2d\x80\x00\x00\x01", // NOLINT
                   21);                                                                        // NOLINT
    std::string r2("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x92\x80\x00\x01\x2e\x80\x00\x00\x02", // NOLINT
                   21);                                                                        // NOLINT
    std::string r3("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x93\x80\x00\x01\x2f\x80\x00\x00\x03", // NOLINT
                   21);                                                                        // NOLINT
    std::string r4("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x94\x80\x00\x01\x30\x80\x00\x00\x04", // NOLINT
                   21);                                                                        // NOLINT
    std::string r5("T200\x00\x80\x00\x00\xc9\x80\x00\x01\x95\x80\x00\x01\x31\x80\x00\x00\x05", // NOLINT
                   21);                                                                        // NOLINT
    std::string b("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x93", 13);                             // NOLINT
    std::string e("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x94", 13);                             // NOLINT
    std::string v("bbb");                                                                      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, r1, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r2, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r3, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r4, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r5, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, b, scan_endpoint::INCLUSIVE, e, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, b, scan_endpoint::INCLUSIVE, e, scan_endpoint::EXCLUSIVE, records));
#endif
    EXPECT_EQ(1, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_range_endpoint2) { // NOLINT
    Storage storage{};
    register_storage(storage);
    // simulating dump failure with jogasaki-tpcc
    std::string r1("CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01", // NOLINT
                   34);                                                                                                             // NOLINT
    std::string r2("CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02", // NOLINT
                   34);                                                                                                             // NOLINT
    std::string r3("CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x03", // NOLINT
                   34);                                                                                                             // NOLINT
    std::string r4("CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01", // NOLINT
                   34);                                                                                                             // NOLINT
    std::string r5("CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02", // NOLINT
                   34);                                                                                                             // NOLINT
    std::string r6("CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x03", // NOLINT
                   34);                                                                                                             // NOLINT
    std::string e("CUSTOMER0\x01", 11);                                                                                             // NOLINT
    std::string v("bbb");                                                                                                           // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, r1, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r2, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r3, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r4, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r5, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, r6, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ScanHandle handle{};
    Tuple* tuple{};
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r1.data(), r1.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r2.data(), r2.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r3.data(), r3.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r4.data(), r4.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r5.data(), r5.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r6.data(), r6.size()), 0);
        EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, r3, scan_endpoint::EXCLUSIVE, e, scan_endpoint::EXCLUSIVE, handle));
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r4.data(), r4.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r5.data(), r5.size()), 0);
#ifdef CPR
        while (Status::OK != read_from_scan(s, handle, &tuple)) {
            ;
        }
#else
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
#endif
        EXPECT_EQ(memcmp(tuple->get_key().data(), r6.data(), r6.size()), 0);
        EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
