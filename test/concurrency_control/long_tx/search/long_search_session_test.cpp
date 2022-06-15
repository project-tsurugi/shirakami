
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

class search_session : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "search_session-search_test");
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
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(search_session, read_version_epoch_ascending_order) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    // prepare data
    epoch::epoch_t ep1{};
    epoch::epoch_t ep2{};
    epoch::epoch_t ep3{};
    auto stop_log_epoch_insert_wait_epoch_update = [s, st](std::string_view k,
                                                           epoch::epoch_t& ep) {
        {
            std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
            ep = epoch::get_global_epoch();
            ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
            ASSERT_EQ(Status::OK, commit(s));
        }
        wait_epoch_update();
    };
    stop_log_epoch_insert_wait_epoch_update(k1, ep1);
    stop_log_epoch_insert_wait_epoch_update(k2, ep2);
    stop_log_epoch_insert_wait_epoch_update(k3, ep3);

    // prepare test
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG));
    wait_epoch_update();

    // test
    auto* ti{static_cast<session*>(s)};
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, k1, buf));
    ASSERT_EQ(ti->get_read_version_max_epoch(), ep1);
    ASSERT_EQ(Status::OK, search_key(s, st, k2, buf));
    ASSERT_EQ(ti->get_read_version_max_epoch(), ep2);
    ASSERT_EQ(Status::OK, search_key(s, st, k3, buf));
    ASSERT_EQ(ti->get_read_version_max_epoch(), ep3);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(ti->get_read_version_max_epoch(), 0);

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(search_session, read_version_epoch_descending_order) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    // prepare data
    epoch::epoch_t ep1{};
    epoch::epoch_t ep2{};
    epoch::epoch_t ep3{};
    auto stop_log_epoch_insert_wait_epoch_update = [s, st](std::string_view k,
                                                           epoch::epoch_t& ep) {
        {
            std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
            ep = epoch::get_global_epoch();
            ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
            ASSERT_EQ(Status::OK, commit(s));
        }
        wait_epoch_update();
    };
    stop_log_epoch_insert_wait_epoch_update(k1, ep1);
    stop_log_epoch_insert_wait_epoch_update(k2, ep2);
    stop_log_epoch_insert_wait_epoch_update(k3, ep3);

    // prepare test
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG));
    wait_epoch_update();

    // test
    auto* ti{static_cast<session*>(s)};
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, k3, buf));
    ASSERT_EQ(ti->get_read_version_max_epoch(), ep3);
    ASSERT_EQ(Status::OK, search_key(s, st, k2, buf));
    ASSERT_EQ(ti->get_read_version_max_epoch(), ep3);
    ASSERT_EQ(Status::OK, search_key(s, st, k1, buf));
    ASSERT_EQ(ti->get_read_version_max_epoch(), ep3);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(ti->get_read_version_max_epoch(), 0);

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
