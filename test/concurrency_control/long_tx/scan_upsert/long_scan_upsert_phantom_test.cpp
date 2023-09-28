
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_scan_upsert_phantom_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "scan_upsert-long_scan_upsert_phantom_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(long_scan_upsert_phantom_test, simple) { // NOLINT
    /**
     * prepare
     */
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1));

    // test
    stop_epoch();
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    resume_epoch();
    wait_epoch_update();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, vb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s1, hd));
    ASSERT_EQ(Status::OK, close_scan(s1, hd));
    ASSERT_EQ(Status::OK, upsert(s1, st, "1", ""));
    // s2 read and forwarding
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd, vb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s2, hd));
    ASSERT_EQ(Status::OK, close_scan(s2, hd));
    ASSERT_EQ(Status::OK, upsert(s2, st, "2", ""));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_NE(Status::OK, commit(s2));
    auto& rinfo = static_cast<session*>(s2)->get_result_info();
    ASSERT_EQ(rinfo.get_reason_code(), reason_code::CC_LTX_PHANTOM_AVOIDANCE);
    ASSERT_EQ(rinfo.get_key(), "2");
    ASSERT_EQ(rinfo.get_storage_name(), "");

    // cleanup
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing