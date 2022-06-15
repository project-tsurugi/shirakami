
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "search_upsert-search_upsert_test");
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

inline void wait_epoch_update() {
    epoch::epoch_t ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce == epoch::get_global_epoch()) {
            _mm_pause();
        } else {
            break;
        }
    }
}

TEST_F(search_upsert, reading_higher_priority_wp) { // NOLINT
    // prepare data and test search on higher priority WP (causing WARN_PREMATURE)
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s0{}; // short
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s0), Status::OK);
    ASSERT_EQ(Status::OK, insert(s0, st, "a", "A"));
    ASSERT_EQ(Status::OK, insert(s0, st, "b", "B"));
    ASSERT_EQ(Status::OK, commit(s0));
    wait_epoch_update();
    // end of data preparation

    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s1, TX_TYPE::LONG, {st}), Status::OK);
    wait_epoch_update();
    ASSERT_EQ(tx_begin(s2, TX_TYPE::LONG, {}), Status::OK);
    wait_epoch_update();
    session* ti1{static_cast<session*>(s1)};
    session* ti2{static_cast<session*>(s2)};
    ASSERT_NE(ti1->get_valid_epoch(), ti2->get_valid_epoch());
    std::string vb{};
    ASSERT_EQ(search_key(s2, st, "a", vb), Status::OK);
    ASSERT_EQ(ti1->get_valid_epoch(), ti2->get_valid_epoch());
    ASSERT_EQ(*ti2->get_overtaken_ltx_set().begin()->second.begin(),
              ti1->get_long_tx_id());
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(search_upsert, reading_lower_priority_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
    }
    Token s1{}; // long
    Token s2{}; // long
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s1, TX_TYPE::LONG, {}), Status::OK);
    ASSERT_EQ(tx_begin(s2, TX_TYPE::LONG, {st}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s1, st, "", vb), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(search_upsert, read_modify_write) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    std::string init_val{"i"};
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK, upsert(s, st, "", init_val));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
        wait_epoch_update(); // For readable if some tx do forewarding.
    }
    std::string s1_val{"s1"};
    std::string s2_val{"s2"};
    {
        // test
        Token s1{}; // long
        Token s2{}; // long
        ASSERT_EQ(enter(s1), Status::OK);
        ASSERT_EQ(enter(s2), Status::OK);
        ASSERT_EQ(tx_begin(s1, TX_TYPE::LONG, {st}), Status::OK);
        ASSERT_EQ(tx_begin(s2, TX_TYPE::LONG, {st}), Status::OK);
        wait_epoch_update();
        std::string vb{};
        ASSERT_EQ(search_key(s1, st, "", vb), Status::OK);
        ASSERT_EQ(vb, init_val);
        ASSERT_EQ(upsert(s1, st, "", s1_val), Status::OK);
        ASSERT_EQ(search_key(s2, st, "", vb), Status::OK); // forewarding
        ASSERT_EQ(vb, init_val);
        ASSERT_EQ(upsert(s2, st, "", s2_val), Status::OK);
        ASSERT_EQ(Status::OK, commit(s1));
        ASSERT_EQ(Status::ERR_VALIDATION, commit(s2));
        ASSERT_EQ(leave(s1), Status::OK);
        ASSERT_EQ(leave(s2), Status::OK);
    }
    {
        // test verify
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        std::string vb{};
        ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
        ASSERT_EQ(vb, s1_val);
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(leave(s), Status::OK);
    }
}

} // namespace shirakami::testing
