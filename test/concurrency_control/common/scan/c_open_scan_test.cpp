
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

TEST_F(open_scan_test, scan_at_non_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_STORAGE_NOT_FOUND,
              open_scan(s, {}, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, open_scan_find_no_index) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    /**
     * It's index scan find own inserting record only.
     */

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, open_scan_find_some_index_nothing_to_read) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s2, st, "", ""));
    ScanHandle hd{};

    // test
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    /**
     * It's index scan find s2's inserting record only, but it is not be able 
     * to read immediately. So it should returns WARN_NOT_FOUND.
     * 
     */

    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(open_scan_test, open_scan_read_own_insert_one) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));

    // test
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    /**
     * It's index scan find own inserting record only.
     */

    ASSERT_EQ(Status::OK, leave(s));
}

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
    auto& sh = ti->get_scan_handle();
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                sh.get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 3);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 1));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                sh.get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 1);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 2));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                sh.get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 2);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 3));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                sh.get_scan_cache()[handle]);
        ASSERT_EQ(scan_buf.size(), 3);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 4));
        auto& scan_buf = std::get<scan_handler::scan_cache_vec_pos>(
                sh.get_scan_cache()[handle]);
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

TEST_F(open_scan_test, multi_open_reading_values) { // NOLINT
    Storage storage{};
    register_storage(storage);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ScanHandle handle2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, "a/1", "1"));
    ASSERT_EQ(Status::OK, insert(s, storage, "b/3", "3"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "a/", scan_endpoint::INCLUSIVE,
                                    "", scan_endpoint::INF, handle));
    ASSERT_EQ(0, handle);

    std::string sb{};
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ("1", sb);

    ASSERT_EQ(Status::OK, open_scan(s, storage, "b/", scan_endpoint::INCLUSIVE,
                                    "", scan_endpoint::INF, handle2));
    ASSERT_EQ(1, handle2);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle2, sb));
    ASSERT_EQ("3", sb);

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
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::OK, next(s, handle));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
