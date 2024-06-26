
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

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_search_upsert_two_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-"
                "search_upsert-long_search_upsert_two_tx_test");
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

// start: concurrent two tx
TEST_F(long_search_upsert_two_tx_test, // NOLINT
       concurrent_search_upsert_txs) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);

    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", std::to_string(2)));
    ASSERT_EQ(Status::OK, commit(s1));

    // test
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s1, st, "", vb), Status::OK); // forwarding
    ASSERT_EQ(vb, std::to_string(2));                  // NOLINT

    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    ASSERT_EQ(search_key(s2, st, "", vb), Status::OK); // forwarding
    ASSERT_EQ(vb, std::to_string(2));                  // NOLINT
    int for_s2 = stoi(vb) + 9;                         // 11 // NOLINT
    int for_s1 = stoi(vb) + 1;                         // 3 // NOLINT

    ASSERT_EQ(search_key(s2, st, "", vb), Status::OK);
    ASSERT_EQ(vb, std::to_string(2)); // NOLINT
    ASSERT_EQ(upsert(s2, st, "", std::to_string(for_s2)), Status::OK);
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2));

    ASSERT_EQ(search_key(s1, st, "", vb), Status::OK);
    ASSERT_EQ(vb, std::to_string(2)); // NOLINT
    ASSERT_EQ(upsert(s1, st, "", std::to_string(for_s1)), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));

    for (;;) {
        auto ret = check_commit(s2);
        if (ret == Status::WARN_WAITING_FOR_OTHER_TX) {
            _mm_pause();
            continue;
        }
        // s2 は s1 に前置してアボートになる。（read を踏む）
        if (ret == Status::ERR_CC) { break; } // other status
        LOG(INFO) << ret;
        break;
    }

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(search_key(s1, st, "", vb), Status::OK);
    ASSERT_EQ(vb, "3");

    // clean up test
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

// end: concurrent two tx

} // namespace shirakami::testing
