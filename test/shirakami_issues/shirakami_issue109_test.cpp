
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;



namespace shirakami::testing {

class shirakami_issue109 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-shirakami_issue109");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(shirakami_issue109, simple) { // NOLINT
    Storage st{};
    ASSERT_OK(create_storage("", st));
    // stop gc
    std::unique_lock<std::mutex> lk{garbage::get_mtx_cleaner()};
    Token s{};
    // prepare record
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "", "v1"));
    ASSERT_OK(commit(s)); // NOLINT
    // wait epoch change
    wait_epoch_update();
    // ltx start and must watch v1
    Token s2{};
    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update(); // s2 can start operation
    // delete v1 by short
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(delete_record(s, st, ""));
    ASSERT_OK(commit(s)); // NOLINT
    // the record become converting
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "", "v2"));
    // s2 must read v1 by scan
    ScanHandle hd{};
    ASSERT_OK(open_scan(s2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(s2, hd, buf));
    ASSERT_EQ(buf, "");
    ASSERT_OK(read_value_from_scan(s2, hd, buf));
    ASSERT_EQ(buf, "v1");
    ASSERT_OK(commit(s2));

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing