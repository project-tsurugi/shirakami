
#include <mutex>
#include <string>

#include "shirakami/interface.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class long_key_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-short_long_key_scan_test");
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

TEST_F(long_key_scan_test, long_key_scan) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k(1024 * 30, 'A'); // works fine with 30K // NOLINT
    //std::string k(1024 * 36, 'A'); // scan failed with 36K
    std::string v("a"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(k, sb);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, sb));
    ASSERT_EQ(v, sb);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, close_scan(s, hd));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
