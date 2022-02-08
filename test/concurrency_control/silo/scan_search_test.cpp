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
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));

    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k2, vb));
    ASSERT_NE("", vb);
    delete_record(s, storage, k2);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    for (;;) {
        auto rc{open_scan(s, storage, k, scan_endpoint::EXCLUSIVE, k4,
                          scan_endpoint::EXCLUSIVE, handle)};
        if (rc != Status::OK) { continue; }
        rc = read_from_scan(s, handle, tuple);
        if (rc != Status::OK) { continue; }
        rc = commit(s);
        if (rc != Status::OK) { continue; }
        _mm_pause();
        break;
    }
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k3));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k4));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
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
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k1, scan_endpoint::INCLUSIVE,
                                    k2, scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    std::string key{};
    tuple->get_key(key);
    ASSERT_EQ(memcmp(key.data(), k1.data(), k1.size()), 0);
    std::string val{};
    tuple->get_value(val);
    ASSERT_EQ(memcmp(val.data(), v1.data(), v1.size()), 0);

    // record exists
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k4, vb));
    ASSERT_EQ(memcmp(vb.data(), v2.data(), v2.size()), 0);

    // record not exist
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k3, vb));

    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    tuple->get_key(key);
    ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
    tuple->get_value(val);
    ASSERT_EQ(memcmp(val.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k2));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k4));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
