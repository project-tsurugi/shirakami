
#include <glog/logging.h>

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

namespace shirakami::testing {

class tsurugi_issue247 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue247");
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

TEST_F(tsurugi_issue247, 20230406_for_comment_kurosawa) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // tx begin s1
    auto s1_work = [s1, st]() {
        ASSERT_EQ(Status::OK, insert(s1, st, "", ""));
        commit(s1); // NOLINT
    };

    auto s2_work = [s2, st]() {
        std::string buf{};
        LOG(INFO) << search_key(s2, st, "", buf);
        // expect warn premature if we insert sleep of stx commit
    };

    auto th_1 = std::thread(s1_work);
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s2, hd));
    for (;;) {
        TxState buf{};
        ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
        if (buf.state_kind() == TxState::StateKind::WAITING_START) {
            _mm_pause();
            continue;
        }
        LOG(INFO) << buf.state_kind();
        break;
    }
    // ltx が dml を実施する。
    auto th_2 = std::thread(s2_work);

    // cleanup
    th_1.join();
    th_2.join();

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing