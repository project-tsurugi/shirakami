
#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue367_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue367_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue367_test, simple) { // NOLINT
    database_options options{};
    int waiting_resolver_threads_num = 16;
    options.set_waiting_resolver_threads(waiting_resolver_threads_num);
    init(options);

    std::uint64_t num_records = 100;
    std::uint64_t num_threads = 8;
    int duration = 1;
    LOG(INFO) << "records_pre tx: " << num_records;
    LOG(INFO) << "duration: " << duration;

    Storage st;
    ASSERT_OK(create_storage("", st));

    auto put = [&](std::uint64_t key, Token t) {
        unsigned char keybuf[8];
        keybuf[0] = key >> 56;
        keybuf[1] = key >> 48;
        keybuf[2] = key >> 40;
        keybuf[3] = key >> 32;
        keybuf[4] = key >> 24;
        keybuf[5] = key >> 16;
        keybuf[6] = key >> 8;
        keybuf[7] = key;
        std::string_view keystr{reinterpret_cast<char*>(keybuf), 8};
        ASSERT_OK(insert(t, st, keystr, keystr));
    };
    std::atomic_uint64_t preparing = num_threads;
    std::atomic_bool end = false;
    std::atomic_uint64_t total_score = 0UL;
    auto work = [&](std::uint64_t& k, std::uint64_t delta) {
        Token t;
        ASSERT_OK(enter(t));
        ASSERT_OK(tx_begin({t, transaction_type::LONG, {st}}));
        --preparing;
        while (preparing != 0) {
            _mm_pause();
            if (end) { break; }
        }
        ++preparing;
        while (epoch::get_global_epoch() <
               static_cast<session*>(t)->get_valid_epoch()) {
            _mm_pause();
        }
        for (std::uint64_t i = 0; i < num_records; i++) {
            put(k, t);
            k += delta;
        }
        Status commit_rc = commit(t);
        while (commit_rc == Status::WARN_WAITING_FOR_OTHER_TX) {
            _mm_pause();
            commit_rc = check_commit(t);
        }
        ASSERT_OK(commit_rc);
        ASSERT_OK(leave(t));
        return;
    };
    auto work2 = [&](std::uint64_t start, std::uint64_t delta) {
        auto k = start;
        std::uint64_t score = 0UL;
        while (!end) {
            work(k, delta);
            score += num_records;
        }
        total_score += score;
        VLOG(20) << "done thread " << start << ", score = " << score;
    };
    std::vector<std::thread> workers;
    workers.reserve(num_threads);
    LOG(INFO) << "start";
    for (std::uint64_t i = 0; i < num_threads; i++) {
        workers.emplace_back(std::thread(work2, i, num_threads));
    }
    std::this_thread::sleep_for(std::chrono::seconds(duration));
    end = true;
    LOG(INFO) << "main thread end";
    for (auto&& elem : workers) { elem.join(); }
    LOG(INFO) << "total score = " << total_score;

    fin();

    ASSERT_EQ(waiting_resolver_threads_num,
              bg_work::bg_commit::joined_waiting_resolver_threads());
}

} // namespace shirakami::testing