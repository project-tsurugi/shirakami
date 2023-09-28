
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/include/wp_meta.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class long_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "scan-long_scan_test");
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

TEST_F(long_scan_test,                                             // NOLINT
       scan_same_storage_concurrent_no_wp_commit_order_high_low) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "1");
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "1");

    ASSERT_EQ(Status::OK, commit(s));  // NOLINT
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_test,                                             // NOLINT
       scan_same_storage_concurrent_no_wp_commit_order_low_high) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "1");
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "1");

    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s));  // NOLINT

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_test,                                          // NOLINT
       scan_same_storage_concurrent_wp_commit_order_high_low) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "1");
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "1");

    ASSERT_EQ(Status::OK, commit(s));  // NOLINT
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_test,                                          // NOLINT
       scan_same_storage_concurrent_wp_commit_order_low_high) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(vb, "1");
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "1");

    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s));                         // NOLINT
    for (;;) {
        auto ret = check_commit(s2);
        if (ret == Status::WARN_WAITING_FOR_OTHER_TX) {
            _mm_pause();
            continue;
        }
        if (ret == Status::OK) { break; } // other status
        ASSERT_EQ(true, false);
    }

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_scan_test,                                     // NOLINT
       some_range_read_register_the_range_if_forwarding) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, upsert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, upsert(s, st, "3", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "1");
    auto* ti = static_cast<session*>(s2);
    wp::wp_meta* wp_meta_ptr{};
    ASSERT_EQ(Status::OK, wp::find_wp_meta(st, wp_meta_ptr));
    auto& range = std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
    ASSERT_EQ(std::get<0>(range), "1");
    ASSERT_EQ(std::get<1>(range), "1");
    ASSERT_EQ(Status::OK, next(s2, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "2");
    ASSERT_EQ(std::get<0>(range), "1");
    ASSERT_EQ(std::get<1>(range), "2");
    ASSERT_EQ(Status::OK, next(s2, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "3");
    ASSERT_EQ(std::get<0>(range), "1");
    ASSERT_EQ(std::get<1>(range), "3");


    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing