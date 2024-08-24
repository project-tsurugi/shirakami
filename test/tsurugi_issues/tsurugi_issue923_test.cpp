
#include <map>

#include "test_tool.h"

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #923: DROP TABLE should call limestone's remove_storage or truncate_storage

namespace shirakami::testing {

class tsurugi_issue923_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tsurugi_issues-tsurugi_issue923");
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

TEST_F(tsurugi_issue923_test, make_lpwal_log) {
    // very fragile test: no way to fully control wal flush timing

    // wait lpwal background worker flushing all
    wait_epoch_update();
    wait_epoch_update();
    lpwal::set_stopping(true);  // stop lpwal background worker
    wait_epoch_update();
    wait_epoch_update();

    std::map<log_operation, int> cnt;
    int cnt_all;

    auto op_count = [&cnt, &cnt_all] {
        for (auto&& es : session_table::get_session_table()) {
            auto& h = es.get_lpwal_handle();
            std::unique_lock<std::mutex> lk{h.get_mtx_logs()};
            for (auto&& l : h.get_logs()) {
                VLOG(10) << l.get_operation();
                cnt[l.get_operation()]++;
                cnt_all++;
            }
        }
    };

    Storage st{};
    ASSERT_OK(create_storage("abc", st));
    cnt.clear();
    cnt_all = 0;
    op_count();
    EXPECT_EQ(cnt[log_operation::UPSERT], 1);
    EXPECT_EQ(cnt[log_operation::ADD_STORAGE], 1);
    EXPECT_EQ(cnt_all, 2);

    // if log buffer is not empty, next internal commit (for delete_record) will flush logs;
    // so make sure it is empty
    for (auto&& es : session_table::get_session_table()) {
        lpwal::flush_log(&es);
    }

    wait_epoch_update();

    ASSERT_OK(delete_storage(st));
    cnt.clear();
    cnt_all = 0;
    op_count();
    EXPECT_EQ(cnt[log_operation::DELETE], 1);
    EXPECT_EQ(cnt[log_operation::REMOVE_STORAGE], 1);
    EXPECT_EQ(cnt_all, 2);
}

} // namespace shirakami::testing
