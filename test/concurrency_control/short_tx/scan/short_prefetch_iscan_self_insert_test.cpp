#include <mutex>
#include <string>

#include "test_tool.h"

#include "shirakami/interface.h"
#include "shirakami/scheme.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class short_prefetch_iscan_self_insert_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-scan-short_prefetch_iscan_self_insert_test");
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        database_options opts{};
        opts.set_iterator_based_scan(true);  // not reproducible if false
        init(opts); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

/**
 * @brief SHORT tx scans the range by open_scan and changes the primary key of each row read, as SQL engine does for
 * `UPDATE t SET c1 = c1 + 10 WHERE c1 >= 10 AND c1 <= 300` (tsurugi-issues#1589).
 * @details the SQL engine reads a row, then deletes the old key and inserts the new key, and reads the next row.
 * The inserts by the tx itself must not be regarded as phantom.
 * In iterator-based-scan mode (the default), open_scan is emulated by open_iscan and prefetch_iscan.
 */
TEST_F(short_prefetch_iscan_self_insert_test, update_primary_key_while_scanning) {
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    // the keys correspond to 10, 200 and 1000
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "k0010", "v"));
    ASSERT_EQ(Status::OK, upsert(s, st, "k0200", "v"));
    ASSERT_EQ(Status::OK, upsert(s, st, "k1000", "v"));
    ASSERT_EQ(Status::OK, commit(s));

    // UPDATE t SET c1 = c1 + 10 WHERE c1 >= 10 AND c1 <= 300
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK,
              open_scan(s, st, "k0010", scan_endpoint::INCLUSIVE, "k0300", scan_endpoint::INCLUSIVE, hd));
    std::string sb{};

    // the first row: 10 -> 20
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k0010");
    ASSERT_EQ(Status::OK, delete_record(s, st, "k0010"));
    ASSERT_EQ(Status::OK, insert(s, st, "k0020", "v"));

    // the second row: 200 -> 210
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb)); // fails with ERR_CC
    EXPECT_EQ(sb, "k0200");
    ASSERT_EQ(Status::OK, delete_record(s, st, "k0200"));
    ASSERT_EQ(Status::OK, insert(s, st, "k0210", "v"));

    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, close_scan(s, hd));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
