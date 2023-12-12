
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class long_concurrent_batch_upsert_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "long_tx-upsert-long_concurrent_batch_upsert_test");
        FLAGS_stderrthreshold = 0;
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

TEST_F(long_concurrent_batch_upsert_test, DISABLED_upsert_ltx100_100) { // NOLINT
    // 100 vs 100

    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));

    // parameter
    constexpr std::size_t th_num{10};
    constexpr std::size_t ops_size{100};
    constexpr std::size_t loop_size{10};
    std::vector<char> readys(th_num);
    std::atomic<bool> go{false};

    // thread func
    auto process = [st, &readys, &go, ops_size, loop_size](std::size_t th_id) {
        // prepare
        Token s{};
        ASSERT_OK(enter(s));

        // ready to ready
        storeRelease(readys.at(th_id), 1);
        // ready
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }
        // go, tx loop
        for (std::size_t i = 0; i < loop_size; ++i) {
            // tx
            ASSERT_OK(tx_begin({s,
                                transaction_options::transaction_type::LONG,
                                {st},
                                {{}, {st}}}));
            auto* ti = static_cast<session*>(s);
            while (ti->get_valid_epoch() > epoch::get_global_epoch()) {
                _mm_pause();
            }

            // ops phase
            for (std::size_t j = 0; j < ops_size; ++j) {
                ASSERT_OK(upsert(s, st, std::to_string(j),
                                 std::to_string(th_id)));
            }

            // commit
            std::atomic<Status> cb_rc{};
            std::atomic<bool> was_committed{false};
            [[maybe_unused]] reason_code rc{};
            [[maybe_unused]] auto cb =
                    [&cb_rc, &rc, &was_committed](
                            Status rs, [[maybe_unused]] reason_code rc_og,
                            [[maybe_unused]] durability_marker_type dm) {
                        cb_rc.store(rs, std::memory_order_release);
                        rc = rc_og;
                        was_committed = true;
                    };
            commit(s, cb);
            // wait commit
            while (!was_committed) { _mm_pause(); }
            ASSERT_OK(cb_rc);
        }
        ASSERT_OK(leave(s));
    };

    // test
    std::vector<std::thread> thv{};
    thv.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) { thv.emplace_back(process, i); }

    wait_for_ready(readys);

    go.store(true, std::memory_order_release);

    LOG(INFO) << "before join";
    for (auto&& th : thv) { th.join(); }
    LOG(INFO) << "after join";

    // verify
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string all_value{};
    for (std::size_t i = 0; i < ops_size; ++i) {
        std::string buf{};
        ASSERT_OK(search_key(s, st, std::to_string(i), buf));
        if (i == 0) {
            all_value = buf;
        } else {
            ASSERT_EQ(all_value, buf);
        }
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing