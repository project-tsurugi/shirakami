
#include <mutex>

#include "concurrency_control/silo/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class open_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-scan-open_scan_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/open_scan_test_log");
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

TEST_F(open_scan_test, max_size_test) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1("k1"); // NOLINT
    std::string k2("k2"); // NOLINT
    std::string k3("k3"); // NOLINT
    std::string v1("v1"); // NOLINT
    std::string v2("v2"); // NOLINT
    std::string v3("v3"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // parepare data
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // open scan
    ScanHandle handle{};
    auto* ti{static_cast<session*>(s)};
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                ti->get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 3);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 1));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                ti->get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 1);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 2));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                ti->get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 2);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 3));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                ti->get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 3);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 4));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                ti->get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 3);
    }
}

TEST_F(open_scan_test, multi_open) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ScanHandle handle2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    ASSERT_EQ(0, handle);
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle2));
    ASSERT_EQ(1, handle2);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, open_scan_test2) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1{"sa"};   // NOLINT
    std::string k2{"sa/"};  // NOLINT
    std::string k3{"sa/c"}; // NOLINT
    std::string k4{"sb"};   // NOLINT
    std::string v{"v"};     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k4, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, k2, scan_endpoint::INCLUSIVE,
                                    "sa0", scan_endpoint::EXCLUSIVE, handle));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
