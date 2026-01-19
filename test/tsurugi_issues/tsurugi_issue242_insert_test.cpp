
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

class tsurugi_issue242_insert_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue242_insert_test");
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

TEST_F(tsurugi_issue242_insert_test, // NOLINT
       insert) {                     // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
                                     /**
        * マルチスレッドで insert
       */
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("a", st));

    constexpr std::size_t th_num{2};
    constexpr std::size_t each_th_rec_num{10};

    Token t{};
    ASSERT_OK(enter(t));

    // test
    ASSERT_OK(tx_begin({t, transaction_type::LONG, {st}}));

    auto tx_begin_wait = [](Token t) {
        TxStateHandle thd{};
        ASSERT_OK(acquire_tx_state_handle(t, thd));
        TxState ts{};
        do {
            ASSERT_OK(check_tx_state(thd, ts));
            std::this_thread::yield();
        } while (ts.state_kind() == TxState::StateKind::WAITING_START);
        ASSERT_OK(release_tx_state_handle(thd));
    };

    tx_begin_wait(t);
    std::vector<char> readys(th_num);
    std::atomic<bool> go{false};
    std::atomic<std::size_t> test_sum{0};
    auto strand_process = [st, t, each_th_rec_num, &readys,
                           &go](std::size_t th_id) {
        // ready to ready
        storeRelease(readys.at(th_id), 1);

        // ready
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }

        // insert
        char c = 'a';
        c += th_id;
        std::string kv_base(&c, 1);
        for (std::size_t i = 0; i < each_th_rec_num; ++i) {
            std::string kv = kv_base + std::to_string(i);
            ASSERT_OK(insert(t, st, kv, kv));
        }

        // cleanup
        LOG(INFO) << "thid " << th_id << " fin.";
    };

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

    // verify
    ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
    ScanHandle shd{};
    ASSERT_OK(open_scan(t, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::size_t read_num{0};
    do {
        std::string key_buf{};
        ASSERT_OK(read_key_from_scan(t, shd, key_buf));
        std::string value_buf{};
        ASSERT_OK(read_value_from_scan(t, shd, value_buf));
        ASSERT_EQ(key_buf, value_buf);
        ++read_num;
    } while (next(t, shd) == Status::OK);
    ASSERT_EQ(read_num, each_th_rec_num * th_num);

    // cleanup
    ASSERT_OK(leave(t));
}

} // namespace shirakami::testing
