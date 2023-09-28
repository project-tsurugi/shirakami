
#include <bitset>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_insert_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "insert_scan-short_insert_scan_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(short_insert_scan_test, insert_find_phantom) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    // s1 does range read.
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s1, hd));
    // s2 does insert.
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s2, st, "k", ""));
    // s3 does insert and find phantom
    ASSERT_EQ(Status::ERR_CC, insert(s1, st, "k2", ""));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_insert_scan_test, scan_read_own_insert) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_insert_scan_test,                      // NOLINT
       scan_find_inserting_record_commit_success) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT, read_key_from_scan(s2, hd, buf));
    // test
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT // success
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_insert_scan_test,                   // NOLINT
       scan_find_inserting_record_commit_fail) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT, read_key_from_scan(s2, hd, buf));
    // test
    ASSERT_EQ(Status::OK, commit(s1));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s2)); // NOLINT // fail

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_insert_scan_test,    // NOLINT
       scan_insert_commit_fail) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    Token s{};

    std::string km1("/");
    std::string k00("0");
    std::string kp1("1");

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));

    ASSERT_EQ(Status::OK, insert(s, st, k00, ""));
    ASSERT_EQ(Status::OK, insert(s, st, kp1, ""));

    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));

    ScanHandle sh{};
    std::string key{};
    std::string val{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, sh));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, sh, key));
    ASSERT_EQ(Status::OK, read_value_from_scan(s, sh, val));
    ASSERT_EQ(key, k00);
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, search_key(s, st, key, val));
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, delete_record(s, st, key));
    ASSERT_EQ(Status::OK, insert(s, st, km1, ""));

    ASSERT_EQ(Status::OK, next(s, sh));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, sh, key));
    ASSERT_EQ(Status::OK, read_value_from_scan(s, sh, val));
    ASSERT_EQ(key, kp1);
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, search_key(s, st, key, val));
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, delete_record(s, st, key));
    ASSERT_EQ(Status::OK, insert(s, st, k00, ""));

    ASSERT_EQ(next(s, sh), Status::WARN_SCAN_LIMIT);

    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_insert_scan_test, // NOLINT
       scan_2nd_insert_fail) { // NOLINT
    // sequence for "UPDATE table SET pk = pk-2"
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    Token s{};

    std::string km2(".");
    std::string km1("/");
    std::string k00("0");
    std::string kp1("1");

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));

    ASSERT_EQ(Status::OK, insert(s, st, k00, ""));
    ASSERT_EQ(Status::OK, insert(s, st, kp1, ""));

    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s}));

    ScanHandle sh{};
    std::string key{};
    std::string val{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, sh));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, sh, key));
    ASSERT_EQ(Status::OK, read_value_from_scan(s, sh, val));
    ASSERT_EQ(key, k00);
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, search_key(s, st, key, val));
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, delete_record(s, st, key));
    ASSERT_EQ(Status::OK, insert(s, st, km2, ""));

    ASSERT_EQ(Status::OK, next(s, sh));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, sh, key));
    ASSERT_EQ(Status::OK, read_value_from_scan(s, sh, val));
    ASSERT_EQ(key, kp1);
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, search_key(s, st, key, val));
    ASSERT_EQ(val, "");

    ASSERT_EQ(Status::OK, delete_record(s, st, key));
    ASSERT_EQ(Status::OK, insert(s, st, km1, ""));

    ASSERT_EQ(next(s, sh), Status::WARN_SCAN_LIMIT);

    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing