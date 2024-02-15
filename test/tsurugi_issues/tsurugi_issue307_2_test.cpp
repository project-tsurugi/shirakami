
#include <mutex>
#include <vector>

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue307_2 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue307_2");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

// for issue#307
// When cc_safe_ss_epoch is much smaller than global_epoch,
// opening two RTX consecutively makes ongoing_tx ill-ordered,
// This causes cc_safe_ss_epoch regression.

TEST_F(tsurugi_issue307_2, DISABLED_ongoing_tx_must_be_ordered) { // NOLINT
    Token sr1{};                                                  // for RTX1
    Token sr2{};                                                  // for RTX2

    auto begin_rtx = [](Token& s) {
        ASSERT_OK(enter(s));
        ASSERT_OK(tx_begin(
                {s, transaction_options::transaction_type::READ_ONLY}));
    };
    auto end_rtx = [](Token& s) {
        ASSERT_OK(commit(s));
        ASSERT_OK(leave(s));
    };

    // setup: make cc_safe_ss_epoch << global_epoch

    Token sl{}; // for LTX (use in setup)
    // begin LTX to keep cc_safe_ss_epoch old
    ASSERT_OK(enter(sl));
    ASSERT_OK(tx_begin({sl, transaction_options::transaction_type::LONG}));

    // advance global_epoch
    for (int i = 0; i < 10; i++) { // NOLINT
        wait_epoch_update();
    }

    // setup done

    epoch::epoch_t at_start = epoch::get_cc_safe_ss_epoch();
    ASSERT_LT(at_start, epoch::get_global_epoch());

    // do the thing

    // end all ltxs/rtxs
    ASSERT_OK(commit(sl));
    ASSERT_OK(leave(sl));
    // ... and begin two rtxs
    begin_rtx(sr1);
    begin_rtx(sr2);

    wait_epoch_update();
    wait_epoch_update();

    epoch::epoch_t after_the_thing = epoch::get_cc_safe_ss_epoch();
    EXPECT_GT(after_the_thing,
              at_start); // cc_safe_ss_epoch is advanced (maybe +10)

    // check1: ongoing_tx::tx_info_ must be ordered (if ill-ordered, bug)
    {
        std::unique_lock<std::shared_mutex> lk(ongoing_tx::get_mtx());
        if (ongoing_tx::get_tx_info().size() == 2) { // check order if no empty
            auto& txi1 = ongoing_tx::get_tx_info()[0];
            auto& txi2 = ongoing_tx::get_tx_info()[1];
            EXPECT_LE(std::get<ongoing_tx::index_epoch>(txi1),
                      std::get<ongoing_tx::index_epoch>(txi2));
        }
    }
    for (int i = 0; i < 9; i++) { wait_epoch_update(); } // NOLINT
    end_rtx(sr1);

    wait_epoch_update();
    wait_epoch_update();

    // check2: cc_safe_ss_epoch must not be regressed (if regressed, bug)
    epoch::epoch_t after_end_rtx1_1 = epoch::get_cc_safe_ss_epoch();
    EXPECT_GE(after_end_rtx1_1, after_the_thing);
    wait_epoch_update();
    // check again
    epoch::epoch_t after_end_rtx1_2 = epoch::get_cc_safe_ss_epoch();
    EXPECT_GE(after_end_rtx1_2, after_end_rtx1_1);
    for (int i = 0; i < 9; i++) { wait_epoch_update(); } // NOLINT
    end_rtx(sr2);
}

} // namespace shirakami::testing
