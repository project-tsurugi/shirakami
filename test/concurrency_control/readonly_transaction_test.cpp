#include <bitset>
#include <thread>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "tuple_local.h"

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
        log_dir.append("/test/readonly_transaction_test_log");
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
    std::this_thread::sleep_for(3s);

    tx_begin(s, true);
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, read_from_scan(s, handle, &tuple));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
