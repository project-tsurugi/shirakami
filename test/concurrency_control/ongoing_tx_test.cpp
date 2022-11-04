
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
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "ongoing_tx_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(ongoing_tx_test, exist_wait_for_test) { // NOLINT
    ongoing_tx::push({1, 1});
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);
    ti->set_long_tx_id(2);
    ti->set_valid_epoch(2);
    Storage st{};
    wp::wp_meta wp_meta{};
    ti->get_wp_set().emplace_back(
            std::make_pair(st, &wp_meta));           // the pair is dummy
    ti->get_overtaken_ltx_set()[&wp_meta].insert(1); // wp_meta is dummy
    ASSERT_EQ(ongoing_tx::exist_wait_for(ti), true);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::exist_wait_for(ti), false);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(ongoing_tx_test, exist_id_test) { // NOLINT
    ASSERT_EQ(ongoing_tx::exist_id(1), false);
    ongoing_tx::push({1, 1});
    ASSERT_EQ(ongoing_tx::exist_id(1), true);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::exist_id(1), false);
}

TEST_F(ongoing_tx_test, get_lowest_epoch_test) { // NOLINT
    // register
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 0);
    ongoing_tx::push({1, 1});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::push({2, 2});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::push({3, 3});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);

    // delete from old
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 2);
    ongoing_tx::remove_id(2);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 3);
    ongoing_tx::remove_id(3);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 0);

    // register again
    ongoing_tx::push({1, 1});
    ongoing_tx::push({2, 2});
    ongoing_tx::push({3, 3});
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);

    // delete from new
    ongoing_tx::remove_id(3);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::remove_id(2);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 1);
    ongoing_tx::remove_id(1);
    ASSERT_EQ(ongoing_tx::get_lowest_epoch(), 0);
}

TEST_F(ongoing_tx_test, change_epoch) { // NOLINT
    ongoing_tx::push({2, 2});
    ongoing_tx::push({3, 3});

    auto& cont = ongoing_tx::get_tx_info();
    for (auto itr = cont.begin(); itr != cont.end(); ++itr) {
        if (itr->second == 2) {
            ASSERT_EQ(itr->first, 2);
        } else if (itr->second == 3) {
            ASSERT_EQ(itr->first, 3);
        } else {
            ASSERT_EQ(true, false); // not reachable
        }
    }

    // fail case
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              ongoing_tx::change_epoch_without_lock(3, 1, 1, 1));

    // success case
    ASSERT_EQ(Status::OK, ongoing_tx::change_epoch_without_lock(3, 2, 2, 2));

    // verify
    for (auto itr = cont.begin(); itr != cont.end(); ++itr) {
        if (itr->second == 2) {
            ASSERT_EQ(itr->first, 2);
        } else if (itr->second == 3) {
            ASSERT_EQ(itr->first, 2);
        } else {
            ASSERT_EQ(true, false); // not reachable
        }
    }
}

} // namespace shirakami::testing
