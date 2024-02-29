
#include <mutex>

#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class ongoing_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "ongoing_tx_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override { std::call_once(init_, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(ongoing_tx_test, exist_wait_for_test) { // NOLINT
    ongoing_tx::push({1, 1, nullptr});
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);
    ti->set_long_tx_id(2);
    ti->set_valid_epoch(2);
    Storage st{};
    wp::wp_meta* wp_meta{};
    ti->get_wp_set().emplace_back(
            std::make_pair(st, wp_meta)); // the pair is dummy
    std::get<0>(ti->get_overtaken_ltx_set()[wp_meta])
            .insert(1); // wp_meta is dummy
                        // ASSERT_EQ(ongoing_tx::exist_wait_for(ti), true);
    ongoing_tx::remove_id(1);
    // ASSERT_EQ(ongoing_tx::exist_wait_for(ti), false);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(ongoing_tx_test, exist_id_test) { // NOLINT
    ASSERT_EQ(ongoing_tx::exist_id(1), false);
    ongoing_tx::push({1, 1, nullptr});
    ASSERT_EQ(ongoing_tx::exist_id(1), true);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::exist_id(1), false);
}

} // namespace shirakami::testing