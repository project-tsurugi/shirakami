
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

class tsurugi_issue319 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue319");
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

TEST_F(tsurugi_issue319, check_write_range_after_submit_commit) { // NOLINT
                                                                  // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));

    Token t1{};
    Token t2{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));

    // test
    ASSERT_OK(
            tx_begin({t1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_OK(
            tx_begin({t2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    std::string buf{};
    ASSERT_EQ(search_key(t2, st, "a", buf), Status::WARN_NOT_FOUND);
    ASSERT_OK(upsert(t2, st, "a", ""));
    ASSERT_OK(upsert(t2, st, "c", ""));
    ASSERT_EQ(commit(t2), Status::WARN_WAITING_FOR_OTHER_TX);
    std::size_t t2_id = static_cast<session*>(t2)->get_long_tx_id();

    // test write range
    wp::page_set_meta* tpsmptr{};
    ASSERT_OK(wp::find_page_set_meta(st, tpsmptr));
    auto* wpmptr = tpsmptr->get_wp_meta_ptr();
    std::string lkey;
    std::string rkey;
    ASSERT_TRUE(wpmptr->read_write_range(t2_id, lkey, rkey));
    ASSERT_EQ(lkey, "a");
    ASSERT_EQ(rkey, "c");

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing