
#include <mutex>

#ifdef WP

#include "concurrency_control/wp/include/session.h"

#else

#include "concurrency_control/silo/include/session.h"

#endif

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class open_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_open_scan_test");
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

TEST_F(open_scan_test, open_scan_skip_head_one_deleted_record) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    {
        std::unique_lock<std::mutex> stop_epoch_for_stop_gc{
                epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, delete_record(s, st, k1));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // test
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k2);
    }
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, open_scan_skip_head_two_deleted_record) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    {
        std::unique_lock<std::mutex> stop_epoch_for_stop_gc{
                epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, delete_record(s, st, k1));
        ASSERT_EQ(Status::OK, delete_record(s, st, k2));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // test
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k3);
    }
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, open_scan_skip_head_three_deleted_record) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k4, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    {
        std::unique_lock<std::mutex> stop_epoch_for_stop_gc{
                epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, delete_record(s, st, k1));
        ASSERT_EQ(Status::OK, delete_record(s, st, k2));
        ASSERT_EQ(Status::OK, delete_record(s, st, k3));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // test
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k4);
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
