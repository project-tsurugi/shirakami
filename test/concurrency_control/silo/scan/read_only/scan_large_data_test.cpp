#include <bitset>

#include "clock.h"

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/snapshot_manager.h"
#include "concurrency_control/silo/include/tuple_local.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class scan_large_data_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "scan-read_only-scan_large_data_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/scan_large_data_test_log");
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(scan_large_data_test, simple_large_data) { // NOLINT
    constexpr static size_t NUM_QUERIES = 20;
    constexpr static size_t NUM_RECORDS = 100;

    Storage storage{};
    register_storage(storage);
    std::string v("0123456789012345678901234567890");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    for (std::size_t i = 0, n = NUM_RECORDS; i < n; ++i) {
        std::string k{std::to_string(i)};
        ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // wait snapshot epoch x 2
    sleepMs(PARAM_SNAPSHOT_EPOCH * PARAM_EPOCH_TIME * 2);

    ASSERT_EQ(Status::OK, tx_begin(s, true));
    std::size_t read_ct{0};
    for (std::size_t i = 0, n = NUM_QUERIES; i < n; ++i) {
        ScanHandle handle{};
        Tuple* tuple{};
        {
            ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF,
                                            "", scan_endpoint::INF, handle));
            for (;;) {
                auto rc{read_from_scan(s, handle, tuple)};
                if (rc == Status::OK) {
                    ++read_ct;
                    continue;
                }
                if (rc == Status::WARN_SCAN_LIMIT) { break; }
                LOG(FATAL);
            }
        }
        ASSERT_EQ(Status::OK, close_scan(s, handle));
    }

    ASSERT_EQ(read_ct, NUM_QUERIES * NUM_RECORDS);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
