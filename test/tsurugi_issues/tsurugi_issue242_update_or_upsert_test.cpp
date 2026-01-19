
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

class tsurugi_issue242_update_or_upsert_test
    : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "complicated-tsurugi_issue242_update_or_upsert_test");
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

INSTANTIATE_TEST_SUITE_P(is_update, tsurugi_issue242_update_or_upsert_test,
                         ::testing::Values(true, false)); // success

TEST_P(tsurugi_issue242_update_or_upsert_test, // NOLINT
       update_or_upsert) {                     // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
                                               /**
        * マルチスレッドで scan-update_or_upsert
       */
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("a", st));

    constexpr std::size_t th_num{2};
    constexpr std::size_t each_th_rec_num{10};

    Token t{};
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
    for (std::size_t i = 0; i < th_num; ++i) {
        char c = 'a';
        c += i;
        std::string kv_base(&c, 1);
        for (std::size_t j = 0; j < each_th_rec_num; ++j) {
            std::string kv = kv_base + std::to_string(j);
            ASSERT_OK(upsert(t, st, kv, kv));
        }
    }
    ASSERT_OK(commit(t));

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
    auto strand_process = [st, t, th_num, each_th_rec_num, &readys,
                           &go](std::size_t th_id) {
        // ready to ready
        storeRelease(readys.at(th_id), 1);

        // ready
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }

        // go
        char l_c = 'a';
        l_c += th_id;
        char r_c = 'a';
        r_c += th_id + 1;
        std::string l_key(&l_c, 1);
        std::string r_key(&r_c, 1);
        ScanHandle shd{};
        ASSERT_OK(open_scan(t, st, l_key, scan_endpoint::INCLUSIVE, r_key,
                            scan_endpoint::EXCLUSIVE, shd));
        std::size_t can_scan_num{0};
        ASSERT_OK(scannable_total_index_size(t, shd, can_scan_num));
        //ASSERT_EQ(can_scan_num, initial_rec_num / th_num);
        std::size_t read_num{0};
        do {
            std::string buf{};
            ASSERT_OK(read_key_from_scan(t, shd, buf));
            if (GetParam()) {
                ASSERT_OK(update(t, st, buf, buf + "u"));
            } else {
                ASSERT_OK(upsert(t, st, buf, buf + "u"));
            }
            ++read_num;
        } while (next(t, shd) == Status::OK);
        ASSERT_EQ(read_num, each_th_rec_num);

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
        ASSERT_EQ(key_buf + "u", value_buf);
        ++read_num;
    } while (next(t, shd) == Status::OK);
    ASSERT_EQ(read_num, each_th_rec_num * th_num);

    // cleanup
    ASSERT_OK(leave(t));
}

} // namespace shirakami::testing
