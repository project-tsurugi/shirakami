
#include <glog/logging.h>

#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue176_3 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue176_3");
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

TEST_F(tsurugi_issue176_3, DISABLED_comment_by_ban_20230228_1730) { // NOLINT
    // create storage
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    // prepare token
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare initial record
    ASSERT_EQ(Status::OK, insert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // delete tx
    ASSERT_EQ(Status::OK, delete_record(s, st, "2"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    auto* ti = static_cast<session*>(s);
    auto deleted_epoch = ti->get_mrc_tid().get_epoch();

    // wait the record is target of gc
    while (!(deleted_epoch < garbage::get_min_step_epoch() &&
             deleted_epoch < garbage::get_min_batch_epoch())) {
        _mm_pause();
    }

    // start full scan tx
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // wait gc
    sleep(1);
    // expecting reference 2 and crash
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing