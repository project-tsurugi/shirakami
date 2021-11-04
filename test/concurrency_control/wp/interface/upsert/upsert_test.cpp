
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
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/upsert_test_log");
        init(false, log_dir); // NOLINT
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

TEST_F(upsert_test, new_epoch_new_version) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    ASSERT_EQ(Status::OK, enter(s));
    auto process = [s](Storage st, bool bt) {
        std::string k{"K"};
        std::string first_v{"v"};
        if (bt) {
            for (;;) {
                auto rc{upsert(s, st, k, first_v)};
                if (Status::OK == rc) { break; }
                _mm_pause();
            }
        } else {
            ASSERT_EQ(upsert(s, st, k, first_v), Status::OK);
        }
        ASSERT_EQ(commit(s), Status::OK);
        epoch::epoch_t ce{epoch::get_global_epoch()};
        while (ce == epoch::get_global_epoch()) { _mm_pause(); }
        std::string second_v{"v2"};
        // Writing after the epoch has changed should be the new version.
        if (bt) {
            ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
            for (;;) {
                auto rc{upsert(s, st, k, second_v)};
                if (Status::OK == rc) { break; }
                _mm_pause();
            }
        } else {
            ASSERT_EQ(upsert(s, st, k, second_v), Status::OK);
        }
        ASSERT_EQ(commit(s), Status::OK);
        std::string_view st_view{reinterpret_cast<char*>(&st), // NOLINT
                                 sizeof(st)};
        Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(st_view, k))};
        ASSERT_NE(rec_d_ptr, nullptr);
        Record* rec_ptr{*rec_d_ptr};
        ASSERT_NE(rec_ptr, nullptr);
        version* ver{rec_ptr->get_latest()};
        ASSERT_NE(ver, nullptr);
        ASSERT_EQ(*ver->get_value(), second_v);
        ver = ver->get_next();
        ASSERT_NE(ver, nullptr);
        ASSERT_EQ(*ver->get_value(), first_v);
    };
    // for occ
    process(st, false);
    LOG(INFO) << "clear about occ";
    // for batch
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
    process(st, true);
    LOG(INFO) << "clear about batch";
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(upsert_test, batch_rmw) { // NOLINT
    const int trial_n{10};
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);

    // begin: initialize table
    std::size_t th_num{5};
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
            while (tx_begin(s, false, true, {st}) != Status::OK) {
                _mm_pause();
            }
            for (auto&& elem : keys) {
                Tuple* tuple{};
                while (search_key(s, st, elem, tuple) != Status::OK) {
                    _mm_pause();
                }
                std::size_t v{};
                memcpy(&v, tuple->get_value().data(), sizeof(v));
                ++v;
                std::string_view v_view{reinterpret_cast<char*>(&v), // NOLINT
                                        sizeof(v)};
                ASSERT_EQ(upsert(s, st, elem, v_view), Status::OK);
            }
            ASSERT_EQ(commit(s), Status::OK);
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
        Tuple* tuple{};
        ASSERT_EQ(search_key(s, st, elem, tuple), Status::OK);
        std::size_t v{};
        memcpy(&v, tuple->get_value().data(), sizeof(v));
        ASSERT_EQ(v, th_num * trial_n);
    }
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
