
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_simple_scan_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/test_log/simple_scan_test_log");
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
    static inline std::string log_dir_; // NOLINT
};

TEST_F(simple_scan, scan_with_prefixed_end) { // NOLINT
    Storage st{};
    register_storage(st);
    std::string k("T6\000\200\000\000\n\200\000\000\001", 11); // NOLINT
    std::string end("T6\001", 3);                              // NOLINT
    std::string v("bbb");                                      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, end,
                                    scan_endpoint::EXCLUSIVE, hd));
    std::size_t ssize{};
    ASSERT_EQ(Status::OK, scannable_total_index_size(s, hd, ssize));
    ASSERT_EQ(ssize, 1);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_range_endpoint1) { // NOLINT
    Storage st{};
    register_storage(st);
    // simulating 1st case in umikongo OperatorTest scan_pushdown_range
    std::string r1("T200\x00\x80\x00\x00\xc7\x80\x00\x01\x91\x80\x00\x01\x2d"
                   "\x80\x00\x00\x01", // NOLINT
                   21);                // NOLINT
    std::string r2("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x92\x80\x00\x01\x2e"
                   "\x80\x00\x00\x02", // NOLINT
                   21);                // NOLINT
    std::string r3("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x93\x80\x00\x01\x2f"
                   "\x80\x00\x00\x03", // NOLINT
                   21);                // NOLINT
    std::string r4("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x94\x80\x00\x01\x30"
                   "\x80\x00\x00\x04", // NOLINT
                   21);                // NOLINT
    std::string r5("T200\x00\x80\x00\x00\xc9\x80\x00\x01\x95\x80\x00\x01\x31"
                   "\x80\x00\x00\x05",                             // NOLINT
                   21);                                            // NOLINT
    std::string b("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x93", 13); // NOLINT
    std::string e("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x94", 13); // NOLINT
    std::string v("bbb");                                          // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, r1, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r2, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r3, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r4, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r5, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, b, scan_endpoint::INCLUSIVE, e,
                                    scan_endpoint::EXCLUSIVE, hd));
    std::size_t ssize{};
    ASSERT_EQ(Status::OK, scannable_total_index_size(s, hd, ssize));
    ASSERT_EQ(ssize, 1);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, scan_range_endpoint2) { // NOLINT
    Storage st{};
    register_storage(st);
    // simulating dump failure with jogasaki-tpcc
    std::string r1(
            "CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00"
            "\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01", // NOLINT
            34);                                            // NOLINT
    std::string r2(
            "CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00"
            "\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02", // NOLINT
            34);                                            // NOLINT
    std::string r3(
            "CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00"
            "\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x03", // NOLINT
            34);                                            // NOLINT
    std::string r4(
            "CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00"
            "\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01", // NOLINT
            34);                                            // NOLINT
    std::string r5(
            "CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00"
            "\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02", // NOLINT
            34);                                            // NOLINT
    std::string r6(
            "CUSTOMER0\x00\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00"
            "\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x03", // NOLINT
            34);                                            // NOLINT
    std::string e("CUSTOMER0\x01", 11);                     // NOLINT
    std::string v("bbb");                                   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, r1, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r2, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r3, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r4, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r5, v));
    ASSERT_EQ(Status::OK, upsert(s, st, r6, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ScanHandle handle{};
    std::string sb{};
    {
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r1.data(), r1.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r2.data(), r2.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r3.data(), r3.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r4.data(), r4.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r5.data(), r5.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r6.data(), r6.size()), 0);
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    {
        ASSERT_EQ(Status::OK, open_scan(s, st, r3, scan_endpoint::EXCLUSIVE, e,
                                        scan_endpoint::EXCLUSIVE, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r4.data(), r4.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r5.data(), r5.size()), 0);
        ASSERT_EQ(Status::OK, next(s, handle));
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(memcmp(sb.data(), r6.data(), r6.size()), 0);
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
