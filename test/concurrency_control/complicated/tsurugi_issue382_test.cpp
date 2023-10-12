
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue382 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue382");
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

TEST_F(tsurugi_issue382, simple) { // NOLINT
                                   // commit ordre is reverse of case 1
    // prepare
    Storage tb1{};
    ASSERT_EQ(Status::OK, create_storage("tb1", tb1));
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, tb1, "1", "100"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    // tb 1 is not null

    // tb2 is null
    Storage tb2{};
    ASSERT_EQ(Status::OK, create_storage("tb2", tb2));

    // tx1 begin
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {tb2}}));
    wait_epoch_update();
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, tb1, "1", buf));
    ASSERT_EQ(buf, "100");
    ASSERT_EQ(Status::OK, insert(s1, tb2, "1", "200"));

    // tx 2 begin
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {tb1}}));
    wait_epoch_update();
    ScanHandle hd2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s2, tb2, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd2));
    ASSERT_EQ(Status::OK, update(s2, tb1, "1", "200"));

    std::atomic<Status> cb_rc{};
    std::atomic<bool> was_called{false};
    auto cb = [&cb_rc,
               &was_called](Status rs, [[maybe_unused]] reason_code rc,
                            [[maybe_unused]] durability_marker_type dm) {
        cb_rc.store(rs, std::memory_order_release);
        was_called.store(true, std::memory_order_release);
    };

    ASSERT_FALSE(commit(s2, cb)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    while (!was_called.load(std::memory_order_acquire)) { _mm_pause(); }

    ASSERT_EQ(Status::ERR_CC, cb_rc.load(std::memory_order_acquire));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing