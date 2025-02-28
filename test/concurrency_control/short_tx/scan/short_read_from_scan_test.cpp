
#include <string.h>
#include <string>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"
#include "test_tool.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, read_from_scan) { // NOLINT
    Storage st{};
    create_storage("", st);
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
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "", v1));
    ASSERT_EQ(Status::OK, insert(s, st, k, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k2, v2));
    ASSERT_EQ(Status::OK, insert(s, st, k3, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k5, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k6, v2));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    /**
     * test
     * if read_from_scan detects self write(update, insert), it read from owns.
     */
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if it calls read_from_scan with invalid handle, it returns
     * Status::ERR_INVALID_HANDLE. if read_from_scan read all records in cache
     * taken at open_scan, it returns Status::WARN_SCAN_LIMIT.
     */
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    //ASSERT_EQ(Status::WARN_INVALID_HANDLE, read_key_from_scan(s, 3, sb)); // DISABLED: using a random number as a pointer causes UB
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k.data(), k.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v1.data(), v1.size()), 0);
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k3.data(), k3.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v1.data(), v1.size()), 0);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    /**
     * test
     * if read_from_scan detects the record deleted by myself, it function
     * returns Status::WARN_NOT_FOUND
     */
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::WARN_NOT_FOUND, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, abort(s));

    /**
     * test
     * if read_from_scan detects the record deleted by others between open_scan
     * and read_from_scan, it function returns Status::WARN_CONCURRENT_DELETE which
     * means reading deleted record.
     */
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s, st, k, scan_endpoint::INCLUSIVE, k4,
                                    scan_endpoint::INCLUSIVE, handle));
    // range : k, k2, k3
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s2, st, k));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::WARN_NOT_FOUND, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(simple_scan, reach_limit) { // NOLINT
    // regression test
    // after next() returns SCAN_LIMIT, read_from_scan() causes UB (buffer overflow) in old shirakami
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));
    for (std::size_t i = 1; i < 256; i++) {
        // insert another entry
        ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_OK(insert(s, st, std::to_string(i), "v"));
        ASSERT_OK(commit(s));

        ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
        ScanHandle handle{};
        ASSERT_OK(open_scan(s, st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, handle));
        std::uint64_t len;
        ASSERT_OK(scannable_total_index_size(s, handle, len));
        ASSERT_EQ(len, i);
        while (true) {
            auto rc = next(s, handle);
            if (rc == Status::OK) { continue; } // ignore records
            ASSERT_EQ(rc, Status::WARN_SCAN_LIMIT);
            break;
        }
        std::string sb{};
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_value_from_scan(s, handle, sb));
        ASSERT_OK(close_scan(s, handle));
        ASSERT_OK(commit(s));
    }
}

TEST_F(simple_scan, exceed_limit) { // NOLINT
    // regression test
    // after next() returns SCAN_LIMIT, read_from_scan() causes UB (buffer overflow) in old shirakami
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "1", "1"));
    ScanHandle handle{};
    ASSERT_OK(open_scan(s, st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, handle));
    // ignore first entry
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
    std::string sb{};
    for (unsigned int i = 0; i < 0x10000; i++) {
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_value_from_scan(s, handle, sb));
    }
    ASSERT_OK(abort(s));
}

} // namespace shirakami::testing
