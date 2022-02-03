
#include <mutex>

#include "clock.h"

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "scan-read_only-ro_read_from_scan_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/ro_read_from_scan_test_log");
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

TEST_F(simple_scan, read_from_scan) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1("k1"); // NOLINT
    std::string k2("k2"); // NOLINT
    std::string k3("k3"); // NOLINT
    std::string v1("v1"); // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s1, storage, k1, v1));
    ASSERT_EQ(Status::OK, insert(s1, storage, k2, v1));
    ASSERT_EQ(Status::OK, insert(s1, storage, k3, v1));

    /**
     * try new scan, but no change about snapshot epoch, so it can't read 
     * anything.
     */
    ASSERT_EQ(tx_begin(s2, true), Status::OK);
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s2, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    Tuple* tuple{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT,
              read_from_scan(s2, handle, tuple));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    LOG(INFO) << "snapshot epoch "
              << snapshot_manager::get_snap_epoch(epoch::get_global_epoch());
    ASSERT_EQ(tx_begin(s2, true), Status::OK); // stop progressing of epoch
    ASSERT_EQ(Status::OK, commit(s1));         // NOLINT

    // check no snapshot
    ASSERT_EQ(Status::OK, open_scan(s2, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s2, handle, tuple));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // wait 2 snapshot epoch
    sleepMs(PARAM_SNAPSHOT_EPOCH * PARAM_EPOCH_TIME * 2);

    LOG(INFO) << "snapshot epoch "
              << snapshot_manager::get_snap_epoch(epoch::get_global_epoch());

    ASSERT_EQ(tx_begin(s1, true), Status::OK);
    ASSERT_EQ(Status::OK, open_scan(s1, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, read_from_scan(s1, handle, tuple));
    std::string key{};
    tuple->get_key(key);
    ASSERT_EQ(memcmp(key.data(), k1.data(), k1.size()), 0);
    ASSERT_EQ(Status::OK, read_from_scan(s1, handle, tuple));
    tuple->get_key(key);
    ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
    ASSERT_EQ(Status::OK, read_from_scan(s1, handle, tuple));
    tuple->get_key(key);
    ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
    ASSERT_EQ(Status::OK, close_scan(s1, handle));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
