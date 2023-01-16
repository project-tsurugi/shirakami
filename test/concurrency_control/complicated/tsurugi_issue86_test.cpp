
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

namespace shirakami::testing {

class tsurugi_issue86 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue86");
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

TEST_F(tsurugi_issue86, comment_221130) { // NOLINT
    Storage st{};
    for (std::size_t i = 0; i < 30; ++i) {
        // drop table
        if (i == 0) {
            // no storage
            ASSERT_EQ(Status::WARN_INVALID_HANDLE, delete_storage(st));
        } else {
            // exist test storage
            ASSERT_EQ(Status::OK, delete_storage(st));
        }

        // create table
        ASSERT_EQ(Status::OK, create_storage("test", st));

        // prepare
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));

        // insert 4 records commit
        ASSERT_EQ(Status::OK, insert(s, st, "a", "a"));
        ASSERT_EQ(Status::OK, insert(s, st, "b", "b"));
        ASSERT_EQ(Status::OK, insert(s, st, "c", "c"));
        ASSERT_EQ(Status::OK, insert(s, st, "d", "d"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // delete 1 records commit (*1)
        ScanHandle hd{};
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        std::size_t scan_count{0};
        do {
            std::string buf{};
            ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
            ++scan_count;
        } while (next(s, hd) == Status::OK);
        ASSERT_EQ(scan_count, 4);
        ASSERT_EQ(Status::OK, close_scan(s, hd));
        ASSERT_EQ(Status::OK, delete_record(s, st, "a"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // delete 1 records which is same to (*1) and commit.
        ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, hd));
        scan_count = 0;
        do {
            std::string buf{};
            ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
            ++scan_count;
        } while (next(s, hd) == Status::OK);
        ASSERT_EQ(scan_count, 3);
        ASSERT_EQ(Status::OK, close_scan(s, hd));
        // not found because already deleted
        ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, st, "a"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        // problem: one ERR_PHANTOM per 15

        // cleanup
        ASSERT_EQ(Status::OK, leave(s));
    }
}

} // namespace shirakami::testing