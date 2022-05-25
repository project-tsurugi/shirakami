/**
 * @file scan_search_test.cpp
 */

#include <xmmintrin.h>

#include <bitset>

#include "gtest/gtest.h"

#include "concurrency_control/include/tuple_local.h"

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
        init(); // NOLINT
    }

    void TearDown() override {
        fin();
    }
};


TEST_F(scan_search, scan_key_search_key) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("a");    // NOLINT
    std::string k2("aa");  // NOLINT
    std::string k3("aac"); // NOLINT
    std::string k4("b");   // NOLINT
    std::string v("bbb");  // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, upsert(s, storage, k4, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::EXCLUSIVE, k4,
                                    scan_endpoint::EXCLUSIVE, handle));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, vb));
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, vb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));

    ASSERT_EQ(Status::OK, search_key(s, storage, k2, vb));
    ASSERT_NE("", vb);
    delete_record(s, storage, k2);
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, tx_begin(s2)); // for delay unhook of k2
    ASSERT_EQ(Status::OK, commit(s));    // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::EXCLUSIVE, k4,
                                    scan_endpoint::EXCLUSIVE, handle));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k3));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k4));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(scan_search, mixing_scan_and_search) { // NOLINT
    Storage storage{};
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
    std::string sb{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k1, scan_endpoint::INCLUSIVE,
                                    k2, scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k1.data(), k1.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v1.data(), v1.size()), 0);

    // record exists
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k4, vb));
    ASSERT_EQ(memcmp(vb.data(), v2.data(), v2.size()), 0);

    // record not exist
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k3, vb));

    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ(memcmp(sb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k2));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k4));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
