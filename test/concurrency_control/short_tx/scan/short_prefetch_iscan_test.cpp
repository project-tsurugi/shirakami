
#include <mutex>
#include <cstring>
#include <string>

#include "test_tool.h"

#include "shirakami/interface.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

#include "concurrency_control/include/scan.h"

namespace shirakami::testing {

using namespace shirakami;

class prefetch_iscan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-scan-short-prefetch_iscan_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(prefetch_iscan_test, limit) {
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));

    // setup
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "k1", "v"));
    ASSERT_OK(upsert(s, st, "k2", "v"));
    ASSERT_OK(upsert(s, st, "k3", "v"));
    ASSERT_OK(commit(s));

    ScanHandle hd{};
    std::string sb{};
    std::size_t size{};

    // forward order, small limit
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(prefetch_iscan(s, hd, 1)); // first elem + 1 more elem
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k1");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k2");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // only reads 2 elems
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(2, size);
    ASSERT_OK(commit(s));

    // reverse order, small limit
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, true, hd));
    ASSERT_OK(prefetch_iscan(s, hd, 1)); // first elem + 1 more elem
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k3");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k2");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // only reads 2 elems
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(2, size);
    ASSERT_OK(commit(s));

    // forward order, bigger limit than num of entries
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(prefetch_iscan(s, hd, 10)); // first elem + 10 more elems
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k1");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k2");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k3");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // after all entries
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(3, size);
    ASSERT_OK(commit(s));

    // reverse order, bigger limit than num of entries
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, true, hd));
    ASSERT_OK(prefetch_iscan(s, hd, 10)); // first elem + 10 more elems
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k3");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k2");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // after all entries
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(3, size);
    ASSERT_OK(commit(s));

    // forward order, limit size 0
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(prefetch_iscan(s, hd, 0)); // first elem + no more elems
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // only reads 1 elem
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(1, size);
    ASSERT_OK(commit(s));

    // reverse order, limit size 0
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, true, hd));
    ASSERT_OK(prefetch_iscan(s, hd, 0)); // first elem + no more elems
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k3");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // only reads 1 elem
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(1, size);
    ASSERT_OK(commit(s));

    ASSERT_OK(leave(s));
}

TEST_F(prefetch_iscan_test, insert_after_scan) {
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));

    // setup
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "k1", "v"));
    ASSERT_OK(upsert(s, st, "k2", "v"));
    ASSERT_OK(commit(s));

    ScanHandle hd{};
    std::string sb{};
    std::size_t size{};

    // forward order, bigger limit than num of entries, insert greater entry after scan
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(prefetch_iscan(s, hd, SIZE_MAX)); // all
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k1");
    ASSERT_OK(upsert(s, st, "k3", "v"));
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k2");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // finished read all entries. k3 is not return
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    ASSERT_EQ(2, size);
    ASSERT_OK(commit(s));

    // setup again
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(delete_record(s, st, "k3"));
    ASSERT_OK(commit(s));

    // not affected by whether or not the RecGC has run

    // reverse order, bigger limit than num of entries, insert smaller entry after scan
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, true, hd));
    ASSERT_OK(prefetch_iscan(s, hd, SIZE_MAX)); // all
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k2");
    ASSERT_OK(upsert(s, st, "k0", "v"));
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, sb));
    EXPECT_EQ(sb, "k1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd)); // finished read all entries. k0 is not return
    ASSERT_OK(scannable_total_index_size(s, hd, size));
    //ASSERT_EQ(2, size); // XXX: this count is affected by RecGC
    ASSERT_OK(commit(s));

    ASSERT_OK(leave(s));
}

TEST_F(prefetch_iscan_test, not_begin_error) {
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));

    // setup
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "k1", "v"));
    ASSERT_OK(commit(s));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(abort(s));
    ASSERT_EQ(Status::WARN_NOT_BEGIN, prefetch_iscan(s, hd, SIZE_MAX));

    ASSERT_OK(leave(s));
}

TEST_F(prefetch_iscan_test, wrong_handle_error) {
    Storage st{};
    create_storage("", st);
    Token s{};
    Token s2{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(s2));

    // setup
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "k1", "v"));
    ASSERT_OK(commit(s));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ScanHandle hd2{};
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(open_iscan(s2, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd2));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, prefetch_iscan(s, hd2, SIZE_MAX));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, prefetch_iscan(s2, hd, SIZE_MAX));
    ASSERT_OK(abort(s));
    ASSERT_OK(abort(s2));

    ASSERT_OK(leave(s));
    ASSERT_OK(leave(s2));
}

TEST_F(prefetch_iscan_test, twice_error) {
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));

    // setup
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "k1", "v"));
    ASSERT_OK(commit(s));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_OK(open_iscan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, hd));
    ASSERT_OK(prefetch_iscan(s, hd, SIZE_MAX));
    // calling prefetch_iscan twice is forbidden
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, prefetch_iscan(s, hd, SIZE_MAX));
    ASSERT_OK(abort(s));

    ASSERT_OK(leave(s));
}

TEST_F(prefetch_iscan_test, vscan_error) {
    if (get_scan_mode_iterator_based()) { GTEST_SKIP() << "this test is for vscan"; }
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));

    // setup
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "k1", "v"));
    ASSERT_OK(commit(s));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_OK(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, hd)); // assume not emulated by iscan
    ASSERT_FALSE(static_cast<scan_context*>(hd)->is_iscan());
    // calling prefetch_iscan for vscan context is forbidden
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, prefetch_iscan(s, hd, SIZE_MAX));
    ASSERT_OK(abort(s));

    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
