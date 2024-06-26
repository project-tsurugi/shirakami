
#include <emmintrin.h>
#include <algorithm>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>
#include <cstring>
#include <ostream>
#include <string>
#include <string_view>

#include "atomic_wrapper.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert_mth : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "search_upsert-short_search_upsert_mth_test");
        // FLAGS_stderrthreshold = 0;
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
    return std::all_of(readys.begin(), readys.end(),
                       [](char b) { return b != 0; });
}

static void wait_for_ready(const std::vector<char>& readys) {
    while (!is_ready(readys)) { _mm_pause(); }
}

TEST_F(search_upsert_mth, rmw) { // NOLINT
    // multi thread rmw tests.

    // generate keys and table
    Storage storage{};
    ASSERT_EQ(create_storage("", storage), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    std::size_t thread_num{5}; // NOLINT
    if (CHAR_MAX < thread_num) { thread_num = CHAR_MAX; }
    LOG(INFO) << "thread num : " << thread_num;
    std::vector<std::string> keys(thread_num);
    for (auto&& elem : keys) {
        static char c{0};
        elem = std::string(1, c);
        ++c;
        std::size_t v{0};
        std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                                sizeof(v)};
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(upsert(s, storage, elem, v_view), Status::OK);
        ASSERT_EQ(Status::OK, commit(s));
    }
    ASSERT_EQ(leave(s), Status::OK);

    std::vector<char> readys(thread_num);
    for (auto&& elem : readys) { elem = 0; }
    std::atomic<bool> go{false};

    auto process = [storage, &go, &readys, &keys](std::size_t th_num) {
        //std::random_device rd;
        //std::mt19937_64 engine(rd());
        //std::shuffle(keys.begin(), keys.end(), engine);
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        storeRelease(readys.at(th_num), 1);
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }
        for (auto&& elem : keys) {
            for (;;) {
                ASSERT_EQ(Status::OK,
                          tx_begin({s, transaction_options::transaction_type::
                                               SHORT}));
                std::string vb{};
                while (search_key(s, storage, elem, vb) != Status::OK) {
                    _mm_pause();
                }
                std::size_t v{};
                memcpy(&v, vb.data(), sizeof(v));
                ++v;
                std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                                        sizeof(v)};
                ASSERT_EQ(upsert(s, storage, elem, v_view), Status::OK);
                if (commit(s) == Status::OK) { break; }
            }
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
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    for (auto&& elem : keys) {
        std::string vb{};
        ASSERT_EQ(search_key(s, storage, elem, vb), Status::OK);
        std::size_t v{};
        memcpy(&v, vb.data(), sizeof(v));
        ASSERT_EQ(v, thread_num);
    }
    commit(s);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
