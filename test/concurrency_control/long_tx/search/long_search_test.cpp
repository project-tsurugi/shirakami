
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/include/wp_meta.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_search_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "search-long_search_test");
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

TEST_F(long_search_test, read_write_mode_single_long_search_success) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    wait_epoch_update();

    // test
    // read only mode and long tx mode, single search
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(long_search_test,                               // NOLINT
       some_search_key_range_register_if_forwarding) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, upsert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, upsert(s, st, "3", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s2, st, "1", vb));
    auto* ti = static_cast<session*>(s2);
    wp::wp_meta* wp_meta_ptr{};
    ASSERT_EQ(Status::OK, wp::find_wp_meta(st, wp_meta_ptr));
    auto& range = std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]);
    ASSERT_EQ(std::get<0>(range), "1");
    ASSERT_EQ(std::get<1>(range), "1");
    ASSERT_EQ(Status::OK, search_key(s2, st, "2", vb));
    ASSERT_EQ(std::get<0>(range), "1");
    ASSERT_EQ(std::get<1>(range), "2");
    ASSERT_EQ(Status::OK, search_key(s2, st, "3", vb));
    ASSERT_EQ(std::get<0>(range), "1");
    ASSERT_EQ(std::get<1>(range), "3");


    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_search_test,                                              // NOLINT
       search_same_storage_concurrent_now_wp_commit_order_high_low) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, search_key(s2, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, commit(s));  // NOLINT
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_search_test,                                              // NOLINT
       search_same_storage_concurrent_now_wp_commit_order_low_high) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, search_key(s2, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s));  // NOLINT

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_search_test,                                          // NOLINT
       search_same_storage_concurrent_wp_commit_order_high_low) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, search_key(s2, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, commit(s));  // NOLINT
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

TEST_F(long_search_test,                                          // NOLINT
       search_same_storage_concurrent_wp_commit_order_low_high) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s2{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, search_key(s2, st, "1", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::WARN_WAITING_FOR_OTHER_TX, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, commit(s));                         // NOLINT
    for (;;) {
        auto ret = check_commit(s2);
        if (ret == Status::WARN_WAITING_FOR_OTHER_TX) {
            _mm_pause();
            continue;
        }
        if (ret == Status::OK) { break; }
        LOG(ERROR) << ret;
        ASSERT_EQ(true, false);
    }

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing