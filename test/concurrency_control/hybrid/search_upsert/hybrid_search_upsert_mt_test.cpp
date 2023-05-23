
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert_mt : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-hybrid-"
                                  "search_upsert-hybrid_search_upsert_mt_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
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

TEST_F(search_upsert_mt, rmw) { // NOLINT
    // multi thread rmw tests.

    // generate keys and table
    Storage storage{};
    ASSERT_EQ(create_storage("", storage), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    std::size_t thread_num{3}; // NOLINT
    //if (CHAR_MAX < thread_num) { thread_num = CHAR_MAX; }
    std::vector<std::string> keys(thread_num);

    // prepare data
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
    std::atomic<bool> go{false};
    std::atomic<std::size_t> batch_loop{0};
    std::atomic<std::size_t> occ_loop{0};

    auto process = [storage, &go, &readys, &keys, &batch_loop,
                    &occ_loop](std::size_t th_num, bool bt) {
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        storeRelease(readys.at(th_num), 1);
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }

        for (;;) {
            if (bt) {
                ASSERT_EQ(tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {storage}}),
                          Status::OK);
            }
            for (auto&& elem : keys) {
                for (;;) {
                SHORT_TX_RETRY: // NOLINT
                    if (!bt) {
                        ASSERT_EQ(
                                Status::OK,
                                tx_begin({s, transaction_options::
                                                     transaction_type::SHORT}));
                    }
                    std::string vb{};
                    for (;;) {
                        auto rc{search_key(s, storage, elem, vb)};
                        if (rc == Status::OK) { break; }
                        if (rc == Status::WARN_PREMATURE ||
                            rc == Status::WARN_CONCURRENT_UPDATE) {
                            _mm_pause();
                        } else if (rc == Status::ERR_CC) {
                            ASSERT_EQ(bt, false); // fail only short
                            goto SHORT_TX_RETRY;  // NOLINT
                        } else {
                            LOG(ERROR)
                                    << log_location_prefix << rc << " " << bt;
                        }
                    }
                    std::size_t v{};
                    memcpy(&v, vb.data(), sizeof(v));
                    ++v;
                    std::string_view v_view{
                            reinterpret_cast<char*>(&v), // NOLINT
                            sizeof(v)};
                    ASSERT_EQ(upsert(s, storage, elem, v_view), Status::OK);
                    if (bt || commit(s) == Status::OK) { break; }
                }
            }
            if (bt) {
                ASSERT_EQ(commit(s), Status::OK);
                ++batch_loop;
            } else {
                ++occ_loop;
            }
            if (batch_loop.load(std::memory_order_acquire) > 3) { // NOLINT
                break;
            }
        }

        ASSERT_EQ(leave(s), Status::OK);
    };

    std::vector<std::thread> thv{};
    thv.reserve(thread_num);
    for (std::size_t i = 0; i < thread_num; ++i) {
        if (i == 0) {
            thv.emplace_back(process, i, true);
        } else {
            thv.emplace_back(process, i, false);
        }
    }

    // ready for threads
    wait_for_ready(readys);
    go.store(true, std::memory_order_release);

    for (auto&& th : thv) { th.join(); }

    LOG(INFO) << "batch_loop " << batch_loop;
    LOG(INFO) << "occ_loop " << occ_loop;
    // verify result
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    for (auto&& elem : keys) {
        std::string vb{};
        ASSERT_EQ(search_key(s, storage, elem, vb), Status::OK);
        std::size_t v{};
        memcpy(&v, vb.data(), sizeof(v));
        ASSERT_EQ(v, batch_loop + occ_loop);
    }
    commit(s);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
