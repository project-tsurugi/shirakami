
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "shirakami/interface.h"
#include "test_tool.h"


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

// for issue#323
// ltx/rtx search_key may read inserting record

class tsurugi_issue323
    : public ::testing::TestWithParam<
              std::tuple<transaction_type, transaction_type>> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue323");
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

INSTANTIATE_TEST_SUITE_P( // NOLINT
        TxTypePair, tsurugi_issue323,
        ::testing::Values(std::make_tuple(transaction_type::SHORT,
                                          transaction_type::LONG),
                          std::make_tuple(transaction_type::SHORT,
                                          transaction_type::READ_ONLY),
                          std::make_tuple(transaction_type::LONG,
                                          transaction_type::LONG),
                          std::make_tuple(transaction_type::LONG,
                                          transaction_type::READ_ONLY)));

void wait_start_tx(Token tx) {
    TxStateHandle sth{};
    ASSERT_OK(acquire_tx_state_handle(tx, sth));
    while (true) {
        TxState state;
        ASSERT_OK(check_tx_state(sth, state));
        if (state.state_kind() == TxState::StateKind::STARTED) break;
        _mm_pause();
    }
}

TEST_P(tsurugi_issue323, must_not_read_inserting_record) { // NOLINT
    transaction_type write_tx_type = std::get<0>(GetParam());
    transaction_type read_tx_type = std::get<1>(GetParam());
    // setup
    Storage st{};
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token txw{};
    ASSERT_OK(enter(txw));
    if (write_tx_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({txw, write_tx_type, {st}}));
    } else {
        ASSERT_OK(tx_begin({txw, write_tx_type}));
    }
    wait_start_tx(txw);
    wait_epoch_update();

    Token txr{};
    ASSERT_OK(enter(txr));
    ASSERT_OK(tx_begin({txr, read_tx_type}));
    wait_start_tx(txr);

    std::string_view key{"k"};
    std::string_view value{"v"};
    ASSERT_OK(insert(txw, st, key, value));
    wait_epoch_update();

    std::string got_value;
    auto rc_search = search_key(txr, st, key, got_value);
    if (rc_search == Status::OK) { EXPECT_EQ(got_value, value); }

    ASSERT_OK(abort(txw));
    ASSERT_OK(leave(txw));

    ASSERT_OK(commit(txr));
    ASSERT_OK(leave(txr));
}

} // namespace shirakami::testing
