
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
    for (std::size_t i = 0; i < 30; ++i) { // NOLINT
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
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, insert(s, st, "a", "a"));
        ASSERT_EQ(Status::OK, insert(s, st, "b", "b"));
        ASSERT_EQ(Status::OK, insert(s, st, "c", "c"));
        ASSERT_EQ(Status::OK, insert(s, st, "d", "d"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // delete 1 records commit (*1)
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
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
        for (std::size_t j = 1;; ++j) {
            if (j % 100 == 0) { // NOLINT
                // to reduce log
                LOG(INFO) << j;
            }
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
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
            auto rc = commit(s);
            if (rc == Status::OK) { break; }
            auto* ti = static_cast<session*>(s);
            ASSERT_EQ(ti->get_result_info().get_reason_code(),
                      reason_code::CC_OCC_READ_VERIFY);

            // problem: one ERR_PHANTOM per 15
        }

        // cleanup
        ASSERT_EQ(Status::OK, leave(s));
    }
}

// regression testcase - this scenario caused phantom
TEST_F(tsurugi_issue86, phantom) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("system", st));
    for (std::size_t i = 0; i < 30; ++i) { // NOLINT
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));
        ScanHandle handle{};
        std::vector<std::size_t> def_ids{};
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        if (Status::OK == open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle)) {
            do {
                std::string sb{};
                ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
                def_ids.emplace_back(std::stoul(sb));
                ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
            } while (next(s, handle) == Status::OK);

            ASSERT_EQ(Status::OK, close_scan(s, handle));
        }

        for (auto&& n : def_ids) {
            ASSERT_EQ(Status::OK, upsert(s, st, std::to_string(n), "value"));
        }
        ASSERT_EQ(Status::OK, upsert(s, st, std::to_string(i), "value"));
        auto rc = commit(s);
        if (rc != Status::OK) {
            auto* ti = static_cast<session*>(s);
            LOG(FATAL) << ti->get_result_info().get_reason_code();
        }
        ASSERT_EQ(Status::OK, leave(s));
    }
}

} // namespace shirakami::testing