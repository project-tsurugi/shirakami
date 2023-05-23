
#include <mutex>
#include <thread>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class delete_scan_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_scan_upsert-short_delete_scan_upsert_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(delete_scan_upsert, range_read_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    std::string k("k");   // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v("v");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(upsert(s, st, k2, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}
} // namespace shirakami::testing