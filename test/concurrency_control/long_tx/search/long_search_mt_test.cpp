
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"
#include "concurrency_control/include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_search_mt_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-search-"
                "long_search_mt_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

static bool is_ready(const std::vector<char>& readys) {
    return std::all_of(readys.begin(), readys.end(),
                       [](char b) { return b != 0; });
}

static void wait_for_ready(const std::vector<char>& readys) {
    while (!is_ready(readys)) { _mm_pause(); }
}

TEST_F(long_search_mt_test, batch_rmw) { // NOLINT
    const int trial_n{1};
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);

    // begin: initialize table
    // ==============================
    std::size_t th_num{3}; // NOLINT
    if (CHAR_MAX < th_num) { th_num = CHAR_MAX; }
    std::vector<std::string> keys(th_num);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    for (auto&& elem : keys) {
        static char c{0};
        elem = std::string(1, c);
        ++c;
        std::size_t v{0};
        std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                                sizeof(v)};
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(upsert(s, st, elem, v_view), Status::OK);
        ASSERT_EQ(Status::OK, commit(s));
    }
    // ==============================
    // end: initialize table

    ASSERT_EQ(leave(s), Status::OK);

    std::vector<char> readys(th_num);
    for (auto&& elem : readys) { elem = 0; };
    std::atomic<bool> go{false};

    auto process = [st, &go, &readys, &keys](std::size_t th_num) {
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        storeRelease(readys.at(th_num), 1);
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }
        for (std::size_t i = 0; i < trial_n; ++i) {
            [[maybe_unused]] TX_BEGIN
                : // NOLINT
                  ASSERT_EQ(
                          tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {st}}),
                          Status::OK);
            wait_epoch_update();

            for (auto&& elem : keys) {
                std::string vb{};
                ASSERT_EQ(Status::OK, search_key(s, st, elem, vb));
            }

            // commit phase
            auto rc = commit(s);
            if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                do {
                    rc = check_commit(s);
                    _mm_pause();
                } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
            }
            if (rc == Status::OK) { continue; }
            if (rc == Status::ERR_CC) { goto TX_BEGIN; } // NOLINT
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << rc;
        }
        ASSERT_EQ(leave(s), Status::OK);
    };

    std::vector<std::thread> thv{};
    thv.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) { thv.emplace_back(process, i); }

    wait_for_ready(readys);
    go.store(true, std::memory_order_release);

    LOG(INFO) << "before join";
    for (auto&& th : thv) { th.join(); }
    LOG(INFO) << "after join";

    // verify result
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    for (auto&& elem : keys) {
        std::string vb{};
        ASSERT_EQ(search_key(s, st, elem, vb), Status::OK);
        std::size_t v{};
        memcpy(&v, vb.data(), sizeof(v));
        ASSERT_EQ(v, 0);
    }
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
