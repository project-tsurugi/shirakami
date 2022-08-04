
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <climits>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert_mth_with_sleep : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "search_upsert-short_search_upsert_mth_with_sleep_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_;
};

static bool is_ready(const std::vector<char>& readys) {
    for (const char& b : readys) {
        if (loadAcquire(b) == 0) return false;
    }
    return true;
}

static void wait_for_ready(const std::vector<char>& readys) {
    while (!is_ready(readys)) { _mm_pause(); }
}

TEST_F(search_upsert_mth_with_sleep, rmw) { // NOLINT
    // multi thread rmw with sleep for single pkey ("") tests.
    // 5 thread execute concurrently. After that, single thread
    // execute 5 tx by the same tx contents.

    // generate storage
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);

    // generate single pkey (record) by ""
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    std::size_t v{0};
    std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                            sizeof(v)};
    ASSERT_EQ(upsert(s, st, "", v_view), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(leave(s), Status::OK);

    // prepare multi thread test
    std::size_t thread_num{2}; // NOLINT
    std::vector<char> readys(thread_num);
    for (auto&& elem : readys) { elem = 0; }
    std::atomic<bool> go{false};

    struct S {
        static bool tx_execute(Token token, Storage st) {
            std::string vb{};
            while (search_key(token, st, "", vb) != Status::OK) { _mm_pause(); }
            std::size_t v{};
            memcpy(&v, vb.data(), sizeof(v));
            ++v;
            std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                                    sizeof(v)};
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(100)); // NOLINT
            upsert(token, st, "", v_view);
            return commit(token) == Status::OK;
        }
    };

    static constexpr std::size_t tx_num{5};
    auto process = [st, &go, &readys](std::size_t th_num) {
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        storeRelease(readys.at(th_num), 1);
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }
        for (std::size_t i = 0; i != tx_num; ++i) {
            for (;;) {
                if (S::tx_execute(s, st)) { break; }
                // If it fails commit, retry.
            }
            // commit
        }
        ASSERT_EQ(leave(s), Status::OK);
    };

    std::vector<std::thread> thv{};
    thv.reserve(thread_num);
    for (std::size_t i = 0; i < thread_num; ++i) {
        thv.emplace_back(process, i);
    }

    // ready for threads
    wait_for_ready(readys);
    go.store(true, std::memory_order_release);

    for (auto&& th : thv) { th.join(); }

    // verify result
    ASSERT_EQ(enter(s), Status::OK);
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    memcpy(&v, vb.data(), sizeof(v));
    ASSERT_EQ(v, thread_num * tx_num);
    commit(s);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
