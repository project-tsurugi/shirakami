#include <bitset>

#include "tuple_local.h"
#include "gtest/gtest.h"

#if defined(RECOVERY)

#include "boost/filesystem.hpp"

#endif

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/test/simple_scan_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, scan) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("aaa");   // NOLINT
    std::string k2("aab");  // NOLINT
    std::string k3("aac");  // NOLINT
    std::string k4("aad");  // NOLINT
    std::string k5("aadd"); // NOLINT
    std::string k6("aa");   // NOLINT
    std::string v("bbb");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, "", v));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k6, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, records));
    uint64_t ctr(0);
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3, scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3, scan_endpoint::EXCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k3, scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k6, scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k6, scan_endpoint::EXCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 1);
    for ([[maybe_unused]] auto&& itr : records) {
        if (ctr == 0) {
            // ASSERT_EQ(itr->get_key().data(), nullptr);
            /*
             * key which is nullptr was inserted, but itr->get_key().data() refer
             * record, so not nullptr.
             */
            ++ctr;
        }
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k5, scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_with_prefixed_end) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("T6\000\200\000\000\n\200\000\000\001", 11); // NOLINT
    std::string end("T6\001", 3);                              // NOLINT
    std::string v("bbb");                                      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, end, scan_endpoint::EXCLUSIVE, records));
    EXPECT_EQ(1, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_range_endpoint1) { // NOLINT
    Storage storage;
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
    ASSERT_EQ(Status::OK, scan_key(s, storage, b, scan_endpoint::INCLUSIVE, e, scan_endpoint::EXCLUSIVE, records));
    EXPECT_EQ(1, records.size());
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, open_scan_test) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ScanHandle handle2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    ASSERT_EQ(0, handle);
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle2));
    ASSERT_EQ(1, handle2);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, open_scan_test2) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k1{"sa"};   // NOLINT
    std::string k2{"sa/"};  // NOLINT
    std::string k3{"sa/c"}; // NOLINT
    std::string k4{"sb"};   // NOLINT
    std::string v{"v"};     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k4, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, k2, scan_endpoint::INCLUSIVE, "sa0", scan_endpoint::EXCLUSIVE, handle));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(simple_scan, read_from_scan) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string k2("aab"); // NOLINT
    std::string k3("aac"); // NOLINT
    std::string k4("aad"); // NOLINT
    std::string k5("aae"); // NOLINT
    std::string k6("aa");  // NOLINT
    std::string v1("bbb"); // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, "", v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v2));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k5, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k6, v2));
    ScanHandle handle{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    /**
     * test
     * if read_from_scan detects self write(update, insert), it read from owns.
     */
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
              read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if it calls read_from_scan with invalid handle, it returns
     * Status::ERR_INVALID_HANDLE. if read_from_scan read all records in cache
     * taken at open_scan, it returns Status::WARN_SCAN_LIMIT.
     */
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_from_scan(s, 3, &tuple));
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    EXPECT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    EXPECT_EQ(memcmp(tuple->get_key().data(), k.data(), k.size()), 0);
    EXPECT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);
    EXPECT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    EXPECT_EQ(memcmp(tuple->get_key().data(), k2.data(), k2.size()), 0);
    EXPECT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    EXPECT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    EXPECT_EQ(memcmp(tuple->get_key().data(), k3.data(), k3.size()), 0);
    EXPECT_EQ(memcmp(tuple->get_value().data(), v1.data(), v1.size()), 0);
    EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if read_from_scan detects the record deleted by myself, it function
     * returns Status::WARN_ALREADY_DELETE.
     */
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    EXPECT_EQ(Status::WARN_ALREADY_DELETE, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, abort(s));

    /**
     * test
     * if read_from_scan detects the record deleted by others between open_scan
     * and read_from_scan, it function returns Status::ERR_ILLEGAL_STATE which
     * means reading deleted record.
     */
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, delete_record(s2, storage, k));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    Status ret{read_from_scan(s, handle, &tuple)};
    ASSERT_TRUE(ret == Status::ERR_PHANTOM || ret == Status::WARN_CONCURRENT_DELETE); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(simple_scan, close_scan) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, handle));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_range_endpoint2) { // NOLINT
    Storage storage;
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
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r1.data(), r1.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r2.data(), r2.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r3.data(), r3.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r4.data(), r4.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r5.data(), r5.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r6.data(), r6.size()), 0);
        EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, r3, scan_endpoint::EXCLUSIVE, e, scan_endpoint::EXCLUSIVE, handle));
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r4.data(), r4.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r5.data(), r5.size()), 0);
        ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
        EXPECT_EQ(memcmp(tuple->get_key().data(), r6.data(), r6.size()), 0);
        EXPECT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing