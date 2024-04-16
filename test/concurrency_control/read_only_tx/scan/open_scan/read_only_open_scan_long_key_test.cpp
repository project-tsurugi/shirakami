
#include <bitset>
#include <mutex>
#include <thread>

#include "test_tool.h"

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_open_scan_long_key : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only_tx-"
                "scan-read_only_open_scan_long_key_test");
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

TEST_F(read_only_open_scan_long_key, 30kb_key_open_scan) { // NOLINT
    // open_scan 30KB key
    Storage st{};
    create_storage("", st);
    std::string k(1024 * 30, '0'); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // it uses for left
    ASSERT_EQ(Status::OK,
              tx_begin({
                      s,
                      transaction_options::transaction_type::READ_ONLY,
              }));
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, st, k, scan_endpoint::INCLUSIVE, "",
                        scan_endpoint::INF, hd));
    // it uses for right
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, st, "", scan_endpoint::INF, k,
                        scan_endpoint::INCLUSIVE, hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_open_scan_long_key, 36kb_key_open_scan) { // NOLINT
    // open_scan 36KB key
    Storage st{};
    create_storage("", st);
    std::string k(1024 * 36, '0'); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({
                      s,
                      transaction_options::transaction_type::READ_ONLY,
              }));
    wait_epoch_update();
    ScanHandle hd{};
    // it uses for left
    ASSERT_EQ(Status::WARN_INVALID_KEY_LENGTH,
              open_scan(s, st, k, scan_endpoint::INCLUSIVE, "",
                        scan_endpoint::INF, hd));
    // it uses for right
    ASSERT_EQ(Status::WARN_INVALID_KEY_LENGTH,
              open_scan(s, st, "", scan_endpoint::INF, k,
                        scan_endpoint::INCLUSIVE, hd));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing