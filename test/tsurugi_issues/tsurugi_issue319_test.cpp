
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"

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

TEST_F(tsurugi_issue319, check_read_write_range_after_submit_commit) { // NOLINT
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
    ASSERT_OK(search_key(t2, st, "a", buf));
    ASSERT_OK(upsert(t2, st, "c", ""));
    ASSERT_OK(search_key(t2, st, "c", buf));
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

    // test read range
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        bool was_checked{false};
        for (auto&& elem : read_plan::get_cont()) {
            if (elem.first == t2_id) {
                auto plist = std::get<0>(elem.second);
                ASSERT_EQ(plist.size(), 1);
                ASSERT_EQ(st, std::get<0>((*plist.begin())));
                ASSERT_EQ(true, std::get<1>((*plist.begin())));
                ASSERT_EQ("a", std::get<2>((*plist.begin())));
                ASSERT_EQ(scan_endpoint::INCLUSIVE,
                          std::get<3>((*plist.begin())));
                ASSERT_EQ("c", std::get<4>((*plist.begin())));
                ASSERT_EQ(scan_endpoint::INCLUSIVE,
                          std::get<5>((*plist.begin())));
                was_checked = true;
            }
        }
        ASSERT_TRUE(was_checked);
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing
