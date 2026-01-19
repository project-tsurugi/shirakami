
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue242_ok_error_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue242_ok_error_test");
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

TEST_F(tsurugi_issue242_ok_error_test, // NOLINT
       simple) {                       // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
                                       /**
        * ok x error strand test
        * ltx が commit 前に early abort するのは read area 違反の時のみ
        * シナリオ
        * (first thread) strand a: point read for readable page
        * (second thread) strand b: read area violation
        *
       */
    // prepare

    Storage st{};
    Storage st2{};
    ASSERT_OK(create_storage("a", st));
    ASSERT_OK(create_storage("b", st2));

    constexpr std::size_t th_num{2};

    Token t{};
    ASSERT_OK(enter(t));
    // preapre
    ASSERT_OK(tx_begin({t, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(t, st, "", ""));
    ASSERT_OK(commit(t));

    // test
    ASSERT_OK(tx_begin({t,
                        transaction_options::transaction_type::LONG,
                        {},
                        {{st}, {st2}}}));

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
    auto strand_process = [st, st2, t, th_num](std::size_t th_id) {
        std::size_t ok_num{0};
        if (th_id == 0) {
            // th id 0
            Status rc{};
            do {
                std::string buf{};
                rc = search_key(t, st, "", buf);
                if (rc == Status::OK) { ++ok_num; }
            } while (rc == Status::OK);
            //ASSERT_EQ(rc, Status::ERR_CC);
            // at aborted early by th-id 1,
            // if th-id 0 is in search_key, search_key returns ERR_CC (intended to test),
            // but th-id 0 is not in search_key, next search_key returns WARN_NOT_BEGIN.
            ASSERT_TRUE(rc == Status::ERR_CC || rc == Status::WARN_NOT_BEGIN) << " rc:" << rc;
        } else {
            // th id 1
            std::string buf{};
            ASSERT_EQ(Status::ERR_READ_AREA_VIOLATION,
                      search_key(t, st2, "", buf));
        }

        // cleanup
        LOG(INFO) << "thid " << th_id << " fin.";
        if (th_id == 0) { LOG(INFO) << ok_num; }
    };

    std::vector<std::thread> thv{};
    thv.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) {
        thv.emplace_back(strand_process, i);
    }

    // threads process some operation

    for (auto&& th : thv) { th.join(); }

    // it was already aborted by ERR_CC
    ASSERT_EQ(Status::WARN_NOT_BEGIN, commit(t));

    // cleanup
    ASSERT_OK(leave(t));
}

} // namespace shirakami::testing
