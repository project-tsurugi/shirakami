
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue361 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue361");
        // FLAGS_stderrthreshold = 0;
        init_for_test();
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue361, comment_20240220) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("tb1", st));
    Token t1{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "1", "100"));
    ASSERT_OK(commit(t1));

    // test
    ASSERT_OK(tx_begin({t1, transaction_options::transaction_type::SHORT}));
    ScanHandle shd1{};
    ASSERT_OK(open_scan(t1, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd1));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd1, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t1, shd1));
    ASSERT_OK(close_scan(t1, shd1));
    ASSERT_OK(insert(t1, st, "2", "100"));

    Token t2{};
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_options::transaction_type::SHORT}));
    ScanHandle shd2{};
    ASSERT_OK(open_scan(t2, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd2));
    ASSERT_OK(read_key_from_scan(t2, shd2, buf));
    ASSERT_EQ(buf, "1");
    ASSERT_EQ(Status::OK, next(t2, shd2));
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT,
              read_key_from_scan(t2, shd2, buf)); // for "2"
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t2, shd2));
    ASSERT_OK(close_scan(t2, shd2));
    ASSERT_OK(insert(t2, st, "3", "100"));

    ASSERT_EQ(Status::ERR_CC, commit(t1));
    ASSERT_OK(commit(t2));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

TEST_F(tsurugi_issue361,
       occ_must_not_read_uncommitted_record) { // NOLINT
    std::size_t inserting_num = 0;
    std::size_t ok_num = 0;
    auto count = [&inserting_num, &ok_num](Storage& st) {
        // init
        inserting_num = 0;
        ok_num = 0;

        Token s2{};
        LOG_IF(FATAL, enter(s2) != Status::OK);
        LOG_IF(FATAL, tx_begin({s2}) != Status::OK);
        ScanHandle sh{};
        auto rc = open_scan(s2, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, sh);
        if (rc == Status::OK) {
            do {
                std::string buf{};
                rc = read_key_from_scan(s2, sh, buf);
                LOG_IF(FATAL, rc != Status::OK &&
                                      rc != Status::WARN_CONCURRENT_INSERT);
                if (rc == Status::OK) {
                    ++ok_num;
                } else if (rc == Status::WARN_CONCURRENT_INSERT) {
                    ++inserting_num;
                } else {
                    LOG(FATAL);
                }
            } while (next(s2, sh) == Status::OK);
        } else {
            LOG(FATAL) << "open_scan rc:" << rc;
        }
        LOG_IF(FATAL, commit(s2) != Status::OK);
        LOG_IF(FATAL, leave(s2) != Status::OK);
    };

    Storage st{};
    ASSERT_OK(create_storage("", st));

    Token s{};

    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s}));
    ASSERT_OK(insert(s, st, "1", ""));

    /**
     * occ reads "1" as cc protocl but doesn't read as service, so return code
     * was Status::WARN_CONCURRENT_INSERT.
    */
    count(st);
    ASSERT_EQ(inserting_num, 1);
    ASSERT_EQ(ok_num, 0);

    ASSERT_OK(commit(s)); // 0 -> 1
    ASSERT_OK(leave(s));

    /**
     * occ reads "1" as cc protocl and service. (Status::OK)
    */
    count(st);
    ASSERT_EQ(inserting_num, 0);
    ASSERT_EQ(ok_num, 1);

    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s}));
    ASSERT_OK(insert(s, st, "2", ""));

    /**
     * occ reads "1" as cc protocl and service. (Status::OK)
     * occ reads "2" as cc protocl but doesn't read as service, so return code
     * was Status::WARN_CONCURRENT_INSERT.
    */
    count(st);
    ASSERT_EQ(inserting_num, 1);
    ASSERT_EQ(ok_num, 1);

    ASSERT_OK(commit(s)); // 1 -> 2
    ASSERT_OK(leave(s));

    /**
     * occ reads "1" and "2" as cc protocl and service. (Status::OK)
    */
    count(st);
    ASSERT_EQ(inserting_num, 0);
    ASSERT_EQ(ok_num, 2);
}

} // namespace shirakami::testing
