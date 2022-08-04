
#include <mutex>

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

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
        //FLAGS_stderrthreshold = 0;        // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
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
    create_storage("", st);
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

TEST_F(open_scan_test,                                               // NOLINT
       open_scan_find_some_index_nothing_to_read_due_to_inserting) { // NOLINT
    Storage st{};
    create_storage("", st);
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

TEST_F(open_scan_test,                                             // NOLINT
       open_scan_find_some_index_nothing_to_read_due_to_deleted) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    {
        std::unique_lock<std::mutex> stop_epoch_for_stop_gc(
                epoch::get_ep_mtx());
        ASSERT_EQ(Status::OK, delete_record(s, st, ""));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ScanHandle hd{};

        // test
        ASSERT_EQ(Status::WARN_NOT_FOUND,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
    }

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, open_scan_read_own_insert_one) { // NOLINT
    Storage st{};
    create_storage("", st);
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

TEST_F(open_scan_test, open_scan_test) { // NOLINT
    Storage storage{};
    create_storage("", storage);
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
