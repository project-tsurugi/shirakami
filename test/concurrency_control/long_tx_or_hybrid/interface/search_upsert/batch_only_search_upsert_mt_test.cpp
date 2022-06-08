
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"
#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class batch_only_search_upsert_mt_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-interface-search_upsert-"
                "batch_only_search_upsert_mt_test");
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

static bool is_ready(const std::vector<char>& readys) {
    for (const char& b : readys) {
        if (loadAcquire(b) == 0) return false;
    }
    return true;
}

static void wait_for_ready(const std::vector<char>& readys) {
    while (!is_ready(readys)) { _mm_pause(); }
}

void wait_epoch_update() {
    epoch::epoch_t ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(batch_only_search_upsert_mt_test, batch_rmw) { // NOLINT
    const int trial_n{2};
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);

    // begin: initialize table
    // ==============================
    std::size_t th_num{8}; // NOLINT
    //if (CHAR_MAX < th_num) { th_num = CHAR_MAX; }
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
        ASSERT_EQ(upsert(s, st, elem, v_view), Status::OK);
        ASSERT_EQ(Status::OK, commit(s));
    }
    // ==============================
    // end: initialize table
    LOG(INFO) << "end initialize table";

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
        TX_BEGIN: // NOLINT
            ASSERT_EQ(tx_begin(s, TX_TYPE::LONG, {st}), Status::OK);
            wait_epoch_update();

            for (auto&& elem : keys) {
                std::string vb{};
                for (;;) {
                    auto rc{search_key(s, st, elem, vb)};
                    if (rc == Status::OK) { break; }
                    if (rc == Status::ERR_FAIL_WP) {
                        goto TX_BEGIN; // NOLINT
                    } else {
                        LOG(FATAL);
                    }
                }

                std::size_t v{};
                memcpy(&v, vb.data(), sizeof(v));
                ++v;
                std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                                        sizeof(v)};
                ASSERT_EQ(shirakami::tx_begin(s, TX_TYPE::LONG, {st}),
                          Status::WARN_ALREADY_BEGIN);
                ASSERT_EQ(upsert(s, st, elem, v_view), Status::OK);
            }

            // commit phase
            for (;;) {
                auto rc = commit(s);
                if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                    _mm_pause();
                    continue;
                }
                if (rc == Status::OK) { break; }
                if (rc == Status::ERR_VALIDATION) { goto TX_BEGIN; } // NOLINT
                LOG(FATAL) << rc;
            }
        }
    };

    std::vector<std::thread> thv{};
    thv.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) { thv.emplace_back(process, i); }

    wait_for_ready(readys);
    go.store(true, std::memory_order_release);

    for (auto&& th : thv) { th.join(); }

    // verify result
    ASSERT_EQ(enter(s), Status::OK);
    for (auto&& elem : keys) {
        std::string vb{};
        ASSERT_EQ(search_key(s, st, elem, vb), Status::OK);
        std::size_t v{};
        memcpy(&v, vb.data(), sizeof(v));
        ASSERT_EQ(v, th_num * trial_n);
    }
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
