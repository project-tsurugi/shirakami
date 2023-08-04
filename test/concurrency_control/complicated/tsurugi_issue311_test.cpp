
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue311 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue311");
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

TEST_F(tsurugi_issue311, simple) { // NOLINT
    constexpr std::size_t thread_num = 10;
    constexpr std::size_t loop_num = 100;

    auto worker = [](int tid) {
        Storage st{};
        std::string st_name = "test" + std::to_string(tid);
        // for own stroage
        ASSERT_OK(create_storage(st_name, st));
        Token s{};
        ASSERT_OK(enter(s));
        // prepare original data
        ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_OK(upsert(s, st, "", ""));
        ASSERT_OK(commit(s));
        // do loop task
        for (std::size_t i = 0; i < loop_num; ++i) {
            // wp st, read positive wp, read negative null
            tx_begin({s,
                      transaction_options::transaction_type::LONG,
                      {st},
                      {{st}, {}}});
            // get tx state and wait by it for started
            TxStateHandle sth{};
            ASSERT_OK(acquire_tx_state_handle(s, sth));
            while (true) {
                TxState state;
                ASSERT_OK(check_tx_state(sth, state));
                if (state.state_kind() == TxState::StateKind::STARTED) {
                    break;
                }
                _mm_pause();
            }
            // tx process, rmw st
            std::string buf{};
            ASSERT_OK(search_key(s, st, "", buf));
            ASSERT_OK(update(s, st, "", ""));
            // it must not wait.
            ASSERT_OK(commit(s));
            // release tx state handle
            ASSERT_OK(release_tx_state_handle(sth));
        }
        // cleanup
        ASSERT_OK(leave(s));
    };

    std::vector<std::thread> threads;

    threads.reserve(thread_num);
    for (std::size_t i = 0; i < thread_num; i++) {
        threads.emplace_back(worker, i);
    }

    for (auto& elem : threads) { elem.join(); }
}

} // namespace shirakami::testing