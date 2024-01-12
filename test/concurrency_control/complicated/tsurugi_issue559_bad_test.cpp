
#include <chrono>

#include "concurrency_control/include/session.h"

#include "database/include/logging.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue559_bad_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue559_bad_test");
        set_is_debug_mode(true);
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

    int64_t get_upsert_case() { return upsert_case_; }

    int64_t get_insert_case() { return insert_case_; }

    void set_upsert_case(int64_t tm) { upsert_case_ = tm; }

    void set_insert_case(int64_t tm) { insert_case_ = tm; }

private:
    static inline std::once_flag init_; // NOLINT
    static inline int64_t upsert_case_;
    static inline int64_t insert_case_;
};

TEST_F(tsurugi_issue559_bad_test, DISABLED_simple) { // NOLINT
    // このテストファイルはいずれ削除する。フレームグラフ用
    bool search_and_upsert = true;
    /**
     * debug build, 100 だとパラメーターの違いはほぼ無し。 1000 だと大きく差がある。
     * 55c693f5a61a134f6c24dcec5505d34ebd311a59: 1000: false: 398 ms
     * true: 19506 ms
    */
    constexpr int num_records = 1000;
    constexpr int num_threads = 8;
    constexpr int enable_init = true;
    SDL << "search_and_upsert: " << search_and_upsert;

    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));

    auto put = [&](int key, Token t) {
        auto keystr = std::to_string(key);
        if (search_and_upsert) {
            std::string buf{};
            if (enable_init) {
                ASSERT_OK(search_key(t, st, keystr, buf));
            } else {
                ASSERT_EQ(search_key(t, st, keystr, buf),
                          Status::WARN_NOT_FOUND);
            }
            ASSERT_OK(upsert(t, st, keystr, keystr));
        } else {
            ASSERT_OK(insert(t, st, keystr, keystr));
        }
    };

    // 初期値生成
    if (enable_init) {
        Token t;
        ASSERT_OK(enter(t));
        ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
        for (int i = 0; i < num_records * num_threads; ++i) {
            ASSERT_OK(upsert(t, st, std::to_string(i), std::to_string(i)));
        }
        ASSERT_OK(commit(t));
        ASSERT_OK(leave(t));
    }

    std::atomic_int preparing = num_threads;

    auto work = [&](int start, int delta, int num) {
        Token t;
        ASSERT_OK(enter(t));
        ASSERT_OK(tx_begin({t, transaction_type::LONG, {st}}));
        wait_epoch_update();
        for (int i = 0; i < num; ++i) { put(start + delta * i, t); }
        preparing--;
        while (preparing > 0) { _mm_pause(); }
        auto commit_rc = commit(t);
        while (commit_rc == Status::WARN_WAITING_FOR_OTHER_TX) {
            _mm_pause();
            commit_rc = check_commit(t);
        }
        ASSERT_OK(commit_rc);
        ASSERT_OK(leave(t));
        SDL << "done thread " << start;
    };

    std::vector<std::thread> workers;
    workers.reserve(num_threads);
    SDL << "start exp";
    auto start = std::chrono::system_clock::now();
    for (int i = 0; i < num_threads; ++i) {
        workers.emplace_back(std::thread(work, i, num_threads, num_records));
    }
    for (auto&& elem : workers) { elem.join(); }
    auto end = std::chrono::system_clock::now();
    auto dur = end - start;
    auto msec =
            std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    SDL << "end exp";
    if (search_and_upsert) {
        set_upsert_case(msec);
        int64_t larger = (get_upsert_case() > get_insert_case())
                                 ? get_upsert_case()
                                 : get_insert_case();
        int64_t smaller = (get_upsert_case() < get_insert_case())
                                  ? get_upsert_case()
                                  : get_insert_case();
        ASSERT_LE(larger, smaller * 1.5); // 大きい方でも 1.5 倍以内に収める
    } else {
        set_insert_case(msec);
    }
}

} // namespace shirakami::testing