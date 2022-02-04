
#include <mutex>
#include <thread>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class scan_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-scan_upsert_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/scan_upsert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(scan_upsert, range_read_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));
    std::string k("k");   // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v("v");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    ASSERT_EQ(insert(s, st, k2, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing