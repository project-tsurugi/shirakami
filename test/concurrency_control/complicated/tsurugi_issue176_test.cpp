
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

class tsurugi_issue176 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue176");
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

TEST_F(tsurugi_issue176, comment_by_ban_20230213_1824) { // NOLINT
    // テストシナリオ: 多くのスレッドが同一テーブルで独立したキーに対してRMW をする。

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    std::size_t th_num(std::thread::hardware_concurrency());

    auto worker = [st, th_num](std::size_t th_id,
                               std::atomic<std::size_t>* prepare_num) {
        // prepare
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));
        std::string key{std::to_string(th_id)};
        std::string val{"value"};
        ASSERT_EQ(Status::OK, upsert(s, st, key, val));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        wait_epoch_update();
        ++*prepare_num;
        while (prepare_num->load(std::memory_order_acquire) != th_num) {
            _mm_pause();
        }

        for (std::size_t i = 0; i < 3; ++i) {
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::LONG,
                                {st}}));
            wait_epoch_update();
            std::string buf{};
            ASSERT_EQ(Status::OK, search_key(s, st, key, buf));
            ASSERT_EQ(Status::OK, upsert(s, st, key, val));
            auto rc = commit(s); // NOLINT
            while (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                _mm_pause();
                rc = check_commit(s);
            }
            ASSERT_EQ(rc, Status::OK);
        }

        // leave
        ASSERT_EQ(Status::OK, leave(s));
    };


    // test
    std::atomic<std::size_t> prepare_num{0};
    std::vector<std::thread> th_vc;
    th_vc.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) {
        th_vc.emplace_back(worker, i, &prepare_num);
    }

    for (auto&& elem : th_vc) { elem.join(); }
}

} // namespace shirakami::testing