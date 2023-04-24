
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue106_2 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue106_2");
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

TEST_F(tsurugi_issue106_2, 20230328_comment_tanabe) { // NOLINT
    int n = 99;                                       // NOLINT
    LOG(INFO) << sizeof(tid_word);
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};

    // setup: make
    //   "0" : alive node,
    //   "1" - n : dead node (before GC)
    ASSERT_OK(enter(s));
    ASSERT_OK(insert(s, st, "0", "")); // min
    for (int i = 1; i <= n; i++) {
        ASSERT_OK(insert(s, st, std::to_string(i), ""));
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));
    ASSERT_OK(enter(s));
    for (int i = 1; i <= n; i++) {
        ASSERT_OK(delete_record(s, st, std::to_string(i)));
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));

    // scan delete nodes -> remember not_found
    for (int loop = 5; loop > 0; loop--) { // NOLINT
        ScanHandle scan{};
        ASSERT_OK(enter(s));
        for (;;) {
            ASSERT_EQ(open_scan(s, st, "1", scan_endpoint::INCLUSIVE,
                                std::to_string(n), scan_endpoint::INCLUSIVE,
                                scan),
                      Status::WARN_NOT_FOUND);
            LOG(INFO) << loop;
            usleep(100000); // may run GC // NOLINT
            auto rc = commit(s);
            if (rc == Status::OK) { break; }
            auto* ti = static_cast<session*>(s);
            LOG(INFO) << ti->get_result_info();
        }
        ASSERT_OK(leave(s));
    }
}

} // namespace shirakami::testing