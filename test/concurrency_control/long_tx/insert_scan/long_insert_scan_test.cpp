
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_insert_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "insert_scan-long_insert_scan_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};


TEST_F(long_insert_scan_test,  // NOLINT
       scan_read_own_insert) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG, {st}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
              read_key_from_scan(s, hd, buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
