
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
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "delete_scan_upsert_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/delete_scan_upsert_test_log");
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

TEST_F(delete_scan_upsert, range_read_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));
    std::string k("k");   // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v("v");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(upsert(s, st, k2, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, hd, tuple));
    ASSERT_EQ(Status::OK, read_from_scan(s, hd, tuple));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, hd, tuple));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}
} // namespace shirakami::testing