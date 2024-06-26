
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue242_search_key_test
    : public ::testing::TestWithParam<transaction_type> {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "complicated-tsurugi_issue242_search_key_test");
        // FLAGS_stderrthreshold = 0;
        init_for_test();
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

static bool is_ready(const std::vector<char>& readys) {
    return std::all_of(readys.begin(), readys.end(),
                       [](char b) { return b != 0; });
}

static void wait_for_ready(const std::vector<char>& readys) {
    while (!is_ready(readys)) { _mm_pause(); }
}

INSTANTIATE_TEST_SUITE_P(tx_mode, tsurugi_issue242_search_key_test,
                         ::testing::Values(transaction_type::READ_ONLY,
                                           transaction_type::LONG)); // success

TEST_P(tsurugi_issue242_search_key_test, // NOLINT
       search_key) {                     // NOLINT
    // prepare
    LOG(INFO) << "test about " << GetParam();
    Storage st{};
    ASSERT_OK(create_storage("", st));

    constexpr std::size_t th_num{10};
    constexpr std::size_t initial_rec_num{100};

    Token t{};
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
    for (std::size_t i = 0; i < initial_rec_num; ++i) {
        ASSERT_OK(upsert(t, st, std::to_string(i), std::to_string(i)));
    }
    ASSERT_OK(commit(t));

    std::vector<char> readys(th_num);
    std::atomic<bool> go{false};
    auto ltx_begin_wait = [](Token t) {
        TxStateHandle thd{};
        ASSERT_OK(acquire_tx_state_handle(t, thd));
        TxState ts{};
        do {
            ASSERT_OK(check_tx_state(thd, ts));
            std::this_thread::yield();
        } while (ts.state_kind() == TxState::StateKind::WAITING_START);
        ASSERT_OK(release_tx_state_handle(thd));
    };
    auto strand_process = [st, t, th_num, initial_rec_num, &readys, &go,
                           &ltx_begin_wait](std::size_t th_id) {
        // ready to ready
        storeRelease(readys.at(th_id), 1);

        // ready
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }

        // go
        for (std::size_t i = initial_rec_num / th_num * th_id;
             i < initial_rec_num / th_num * (th_id + 1); ++i) {
            std::string buf{};
            ASSERT_OK(search_key(t, st, std::to_string(i), buf));
        }

        // cleanup
        LOG(INFO) << "thid " << th_id << " fin.";
    };

    // test
    ASSERT_OK(tx_begin({t, GetParam()}));
    ltx_begin_wait(t);
    std::vector<std::thread> thv{};
    thv.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) {
        thv.emplace_back(strand_process, i);
    }

    wait_for_ready(readys);

    go.store(true, std::memory_order_release);

    // threads process some operation

    for (auto&& th : thv) { th.join(); }

    ASSERT_OK(commit(t));

    // cleanup
    ASSERT_OK(leave(t));
}

} // namespace shirakami::testing
