
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

TEST_F(tsurugi_issue247, 20230427_for_comment_ban) { // NOLINT
    fin();
    database_options options{};
    options.set_epoch_time(1);
    init(options);

    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1)); // short
    ASSERT_EQ(Status::OK, enter(s2)); // long

    // make s1 large tx
    for (std::size_t i = 0; i < 1000; ++i) {
        std::string_view key(reinterpret_cast<char*>(&i), sizeof(i)); // NOLINT
        ASSERT_EQ(Status::OK, insert(s1, st, key, ""));
    }

    auto s1_commit_work = [s1, st]() {
        commit(s1); // NOLINT
    };
    auto th_1 = std::thread(s1_commit_work);

    // s1 is high priori against s2
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update(); // epoch change
    TxStateHandle hd{};
    ASSERT_EQ(Status::OK, acquire_tx_state_handle(s2, hd));
    // default is started

    TxState buf{};
    ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    while (buf.state_kind() == TxState::StateKind::WAITING_START) {
        _mm_pause();
        ASSERT_EQ(Status::OK, check_tx_state(hd, buf));
    }
    ASSERT_EQ(buf.state_kind(), TxState::StateKind::STARTED);
    std::string val_buf{};
    ASSERT_NE(Status::WARN_PREMATURE, search_key(s2, st, "", val_buf));

    th_1.join();
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing