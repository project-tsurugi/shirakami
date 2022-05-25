
#include <mutex>


#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/session.h"

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

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(open_scan_test,                   // NOLINT
       open_scan_skip_one_hdr_no_snap) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    {
        // for stop gc
        std::unique_lock mtx_c{garbage::get_mtx_cleaner()};
        {
            // for no snapshot
            std::unique_lock mtx_e{epoch::get_ep_mtx()};
            ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
            ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT

            ASSERT_EQ(Status::OK, delete_record(s, st, k1));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        }

        // test
        ASSERT_EQ(Status::OK, tx_begin(s, false, true, {}));
        wait_change_epoch();
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k2);
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    }
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test,                   // NOLINT
       open_scan_skip_two_hdr_no_snap) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    {
        // for stop gc
        std::unique_lock mtx_c{garbage::get_mtx_cleaner()};
        {
            // for no snapshot
            std::unique_lock mtx_e{epoch::get_ep_mtx()};
            ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
            ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
            ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT

            ASSERT_EQ(Status::OK, delete_record(s, st, k1));
            ASSERT_EQ(Status::OK, delete_record(s, st, k2));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        }

        // test
        ASSERT_EQ(Status::OK, tx_begin(s, false, true, {}));
        wait_change_epoch();
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k3);
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    }
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test,                     // NOLINT
       open_scan_skip_three_hdr_no_snap) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    {
        // for stop gc
        std::unique_lock mtx_c{garbage::get_mtx_cleaner()};
        {
            // for no snapshot
            std::unique_lock mtx_e{epoch::get_ep_mtx()};
            ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
            ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
            ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
            ASSERT_EQ(Status::OK, upsert(s, st, k4, ""));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT

            ASSERT_EQ(Status::OK, delete_record(s, st, k1));
            ASSERT_EQ(Status::OK, delete_record(s, st, k2));
            ASSERT_EQ(Status::OK, delete_record(s, st, k3));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        }

        // test
        ASSERT_EQ(Status::OK, tx_begin(s, false, true, {}));
        wait_change_epoch();
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
        ASSERT_EQ(sb, k4);
        ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
