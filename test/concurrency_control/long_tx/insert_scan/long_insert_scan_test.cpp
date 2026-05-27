
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_insert_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "insert_scan-long_insert_scan_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

// single tx

TEST_F(long_insert_scan_test,  // NOLINT
       scan_read_own_insert) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_insert_scan_test, scan_read_own_insert_on_absent) { // NOLINT
    // test for check_not_found in open_scan/next (with local write set)

    // prepare
    // storage: { "1": absent, "2": alive, "3": absent }
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};

    // stop Record GC
    std::unique_lock<std::mutex> lk{garbage::get_mtx_cleaner()};

    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "3", ""));
    ASSERT_OK(commit(s));
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, "1"));
    ASSERT_EQ(Status::OK, delete_record(s, st, "3"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    // test
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    ltx_begin_wait(s);

    ASSERT_OK(insert(s, st, "1", ""));
    ASSERT_OK(insert(s, st, "3", ""));
    ScanHandle hd{};
    ASSERT_OK(open_scan(s, st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(s, hd, buf)); // read local write set
    EXPECT_EQ(buf, "1");
    ASSERT_OK(next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, buf));
    EXPECT_EQ(buf, "2");
    ASSERT_OK(next(s, hd));
    ASSERT_OK(read_key_from_scan(s, hd, buf)); // read local write set
    EXPECT_EQ(buf, "3");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_OK(commit(s));

    // cleanup
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
