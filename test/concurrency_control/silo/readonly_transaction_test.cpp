#include <bitset>
#include <thread>

#include "gtest/gtest.h"

#include "clock.h"

#include "concurrency_control/silo/include/snapshot_manager.h"
#include "concurrency_control/silo/include/tuple_local.h"

#ifdef CPR

#include "fault_tolerance/include/cpr.h"

#endif

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;
using namespace std::chrono_literals;

Storage storage;

class readonly_transaction_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/readonly_transaction_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(readonly_transaction_test, readonly_scan) { // NOLINT
    register_storage(storage);
    std::string k{"k"}; // NOLINT
    std::string v{"v"}; // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // trying to wait enough
    epoch::epoch_t ce = epoch::get_global_epoch();
    while (snapshot_manager::get_snap_epoch(ce) == snapshot_manager::get_snap_epoch(epoch::get_global_epoch())) {
        sleepMs(1);
    }

    tx_begin(s, true);
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    Tuple* tuple{};
#ifdef CPR
    auto ret = read_from_scan(s, handle, tuple);
    while (ret == Status::WARN_CONCURRENT_UPDATE) {
        ret = read_from_scan(s, handle, tuple);
    }
    ASSERT_EQ(Status::OK, ret);
    ret = read_from_scan(s, handle, tuple);
    while (ret == Status::WARN_CONCURRENT_UPDATE) {
        ret = read_from_scan(s, handle, tuple);
    }
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, ret);
#else
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, tuple));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, tuple));
#endif
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
