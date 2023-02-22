
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_delete_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "delete_scan-long_delete_scan_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(long_delete_scan_test,                   // NOLINT
       delete_against_open_scan_already_read) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(upsert(s1, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s1, st, "b", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "a");
    ASSERT_EQ(Status::OK, delete_record(s1, st, "a"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    // sleep(1);
    ASSERT_EQ(Status::OK, next(s2, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "b");
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_delete_scan_test,                        // NOLINT
       delete_against_open_scan_next_find_deleted) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(upsert(s1, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s1, st, "b", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // delete not read by s2
    ASSERT_EQ(Status::OK, delete_record(s1, st, "b"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    // sleep(1);
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(vb, "a");
    ASSERT_EQ(Status::OK, next(s2, hd));
    // because s2 see before s1 by forwarding
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_delete_scan_test,                             // NOLINT
       delete_against_open_scan_read_scan_find_deleted) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(upsert(s1, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s1, st, "b", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, delete_record(s1, st, "a"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    // sleep(1);
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    // because s2 see before s1 by forwarding
    ASSERT_EQ(Status::OK, next(s2, hd));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing